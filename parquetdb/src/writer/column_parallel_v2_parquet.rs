use std::{fs::File, io::Write, sync::Arc};

use anyhow::Result;
use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::{DataType, Schema},
    error::{Error as ArrowError, Result as ArrowResult},
    io::parquet::write::{
        CompressedPage, CompressionOptions, DynIter, DynStreamingIterator, Encoding,
        FallibleStreamingIterator, Page, SchemaDescriptor, Version, WriteOptions, array_to_columns,
        compress, to_parquet_schema, transverse,
    },
};
use parquet2::write::{
    FileWriter as ParquetFileWriter, Version as ParquetVersion, WriteOptions as ParquetWriteOptions,
};

/// ColumnCompressedIter：单列级“流式压缩”迭代器。
///
/// - 内部持有一个 `DynIter<Result<Page>>`，来自 `array_to_columns`；
/// - `advance()` 时从 encoded_pages 取出下一页并压缩为 `CompressedPage` 存入 `current`；
/// - `get()` 返回当前压缩页的引用。
struct ColumnCompressedIter {
    encoded_pages: DynIter<'static, ArrowResult<Page>>,
    compression: CompressionOptions,
    current: Option<CompressedPage>,
}

impl ColumnCompressedIter {
    fn new(
        encoded_pages: DynIter<'static, ArrowResult<Page>>,
        compression: CompressionOptions,
    ) -> Self {
        Self {
            encoded_pages,
            compression,
            current: None,
        }
    }
}

impl FallibleStreamingIterator for ColumnCompressedIter {
    type Item = CompressedPage;
    type Error = ArrowError;

    fn advance(&mut self) -> ArrowResult<()> {
        match self.encoded_pages.next() {
            Some(Ok(page)) => {
                let compressed = compress(page, Vec::new(), self.compression)
                    .map_err(|e| ArrowError::from(e))?;
                self.current = Some(compressed);
            }
            Some(Err(e)) => {
                self.current = None;
                return Err(e);
            }
            None => {
                self.current = None;
            }
        }
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}

/// ColumnParallelParquetWriter 实现了“RowGroup 内按列并行准备 Page + 写入时流式压缩”的方案。
///
/// - rayon 仅用于并行调用 `array_to_columns`，构造每个 leaf 列的 `DynIter<Result<Page>>`；
/// - 每个 leaf 列包装为 `ColumnCompressedIter`，在 `FileWriter::write(row_group)` 消费时按页压缩；
/// - `close` 时一次性将内存缓冲落盘，保持“大块顺序写”的语义。
pub struct ColumnParallelV2ParquetWriter {
    // Arrow schema
    schema: Arc<Schema>,
    // Parquet 物理 schema（列描述符等）
    parquet_schema: Arc<SchemaDescriptor>,
    // 列级写入选项（压缩、版本、page 大小等）
    options: Arc<WriteOptions>,
    // 每个 Arrow 字段对应的 Parquet 列编码配置
    encodings: Arc<Vec<Vec<Encoding>>>,
    // 实际负责写入 Parquet 的底层 writer，背后是一个 Vec<u8> 缓冲
    writer: ParquetFileWriter<Vec<u8>>,
    // 目标文件句柄，close 时一次性顺序写入
    file: File,
}

impl super::ParquetWriter for ColumnParallelV2ParquetWriter {
    fn try_new(path: &str, schema: Arc<Schema>) -> Result<Self> {
        // Arrow schema -> Parquet 物理 schema（parquet2::metadata::SchemaDescriptor）
        let parquet_schema = to_parquet_schema(&*schema)?;

        // Arrow 侧写入选项：统计 + Zstd 压缩 + V2
        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Zstd(None),
            version: Version::V2,
            data_pagesize_limit: None,
        };

        // 为每个 Arrow 字段派发列级 Encoding，使用 transverse 将嵌套类型展开到叶子列。
        let encoding_map = |_: &DataType| Encoding::Plain;
        let encodings: Vec<Vec<Encoding>> = schema
            .fields
            .iter()
            .map(|f| transverse(&f.data_type, encoding_map))
            .collect();

        // parquet2 FileWriter，将完整 Parquet 内容写入到内存 Vec<u8> 中
        let writer = ParquetFileWriter::new(
            Vec::new(),
            parquet_schema.clone(),
            ParquetWriteOptions {
                write_statistics: true,
                version: ParquetVersion::V2,
            },
            None,
        );

        Ok(Self {
            schema,
            parquet_schema: Arc::new(parquet_schema),
            options: Arc::new(options),
            encodings: Arc::new(encodings),
            writer,
            file: File::create(path)?,
        })
    }

    fn write_batch(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()> {
        use rayon::prelude::*;

        // 将 batch 视为一个 RowGroup。
        let options = *self.options;
        let parquet_schema = (*self.parquet_schema).clone();
        let encodings = (*self.encodings).clone();

        // 并行为每个 Arrow 列准备其所有 leaf 列的 Page 流（DynIter<Result<Page>>）。
        // 此处仅做“编码为 Page”的 CPU 密集工作，不做压缩。
        let column_streams = batch
            .columns()
            .par_iter()
            .zip(parquet_schema.fields().to_vec())
            .zip(encodings.par_iter())
            .flat_map(move |((array, type_), encoding)| {
                let encoded_columns = array_to_columns(array, type_, options, encoding).unwrap();

                encoded_columns
                    .into_iter()
                    .map(|encoded_pages| {
                        // 针对每个 leaf 列构造流式压缩迭代器，再包装成 DynStreamingIterator
                        let iter = ColumnCompressedIter::new(encoded_pages, options.compression);
                        DynStreamingIterator::new(iter)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        // RowGroupIter<'a, E> = DynIter<'a, Result<DynStreamingIterator<'a, CompressedPage, E>, E>>
        let row_group = DynIter::new(
            column_streams
                .into_iter()
                .map(|column_stream| Ok(column_stream)),
        );

        // 交给 parquet2::write::FileWriter 顺序写入该 RowGroup，
        // 压缩会在 ColumnCompressedIter::advance 调用时按页发生。
        self.writer.write(row_group)?;

        Ok(())
    }

    fn close(mut self) -> Result<Self> {
        // 写 footer，完成文件结构。
        self.writer.end(None)?;

        // 取出内存中完整的 Parquet 文件内容，一次性顺序写入到底层文件。
        let buf = self.writer.into_inner();
        self.file.write_all(&buf)?;

        // 为了与 Engine 的 flush 语义兼容，返回一个新的 Self，
        // 其中 Vec<u8> 作为已经写入的前缀，后续写入会继续在其后追加。
        Ok(Self {
            schema: self.schema.clone(),
            parquet_schema: self.parquet_schema.clone(),
            options: self.options,
            encodings: self.encodings,
            writer: ParquetFileWriter::new(
                buf,
                (*self.parquet_schema).clone(),
                ParquetWriteOptions {
                    write_statistics: true,
                    version: ParquetVersion::V2,
                },
                None,
            ),
            file: self.file,
        })
    }
}
