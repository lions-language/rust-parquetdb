use std::{collections::VecDeque, fs::File, io::Write, sync::Arc};

use anyhow::Result;
use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::{DataType, Schema},
    error::{Error as ArrowError, Result as ArrowResult},
    io::parquet::{
        read::ParquetError,
        write::{
            CompressedPage, CompressionOptions, DynIter, DynStreamingIterator, Encoding,
            FallibleStreamingIterator, SchemaDescriptor, Version, WriteOptions, array_to_columns,
            row_group_iter, to_parquet_schema, transverse,
        },
    },
};
use parquet2::write::{
    FileWriter as ParquetFileWriter, Version as ParquetVersion, WriteOptions as ParquetWriteOptions,
};

/// Streaming iterator over a single column chunk (sequence of compressed pages).
/// This matches parquet2::FallibleStreamingIterator<Item = CompressedPage>.
struct Bla {
    columns: VecDeque<CompressedPage>,
    current: Option<CompressedPage>,
}

impl Bla {
    pub fn new(columns: VecDeque<CompressedPage>) -> Self {
        Self {
            columns,
            current: None,
        }
    }
}

impl FallibleStreamingIterator for Bla {
    type Item = CompressedPage;
    type Error = ArrowError;

    fn advance(&mut self) -> ArrowResult<()> {
        self.current = self.columns.pop_front();
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}

/// ColumnParallelParquetWriter 实现了“RowGroup 内按列并行编码 + 单线程合并写入”的真实方案。
///
/// - 每次 `write_batch` 将传入的 `Chunk<Box<dyn Array>>` 视为一个 RowGroup；
/// - 对其中每个 Arrow 列使用 `array_to_columns` 并行生成 Parquet pages，
///   再用 `compress` 压缩成 `CompressedPage`；
/// - 将每个列的 page 序列包装成实现 `FallibleStreamingIterator<Item = CompressedPage>` 的 `Bla`；
/// - 用 `DynStreamingIterator` + `DynIter` 组装成一个 `RowGroupIter`，
///   交给底层 `parquet2::write::FileWriter<Vec<u8>>` 顺序写入；
/// - `close` 时一次性将内存缓冲落盘，保持“大块顺序写”的语义。
pub struct ColumnParallelParquetWriter {
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

impl super::ParquetWriter for ColumnParallelParquetWriter {
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

        // 将 batch 视为一个 RowGroup，按列并行编码。
        // 为了保持接口简单，这里使用 Arrow 的 Error 作为列编码阶段的错误类型。
        let options = *self.options;
        let parquet_schema = (*self.parquet_schema).clone();
        let encodings = (*self.encodings).clone();

        // 参照官方 parallel_write 示例：
        // - array_to_columns：Array + ParquetType + WriteOptions + &[Encoding]
        //   -> Vec<DynIter<Result<Page>>>
        // - compress：Page -> CompressedPage
        // - 每个 Arrow 列可能对应多个叶子列（nested 类型），因此 flat_map 之后得到的是
        //   Vec<VecDeque<CompressedPage>>，每个 VecDeque 对应一个 leaf column chunk。
        let columns: ArrowResult<Vec<VecDeque<CompressedPage>>> = batch
            .columns()
            .par_iter()
            .zip(parquet_schema.fields().to_vec())
            .zip(encodings.par_iter())
            .flat_map(move |((array, type_), encoding)| {
                // // 按官方示例，array_to_columns 返回每个叶子列的一组 Page 迭代器。
                // let encoded_columns = array_to_columns(array, type_, options, encoding).unwrap();

                // encoded_columns
                //     .into_iter()
                //     .map(|encoded_pages| {
                //         // 将 `DynIter<Result<Page>>` 的错误类型转换为 ParquetError，
                //         // 以便后续再统一映射为 ArrowError。
                //         let encoded_pages =
                //             DynIter::new(encoded_pages.into_iter().map(|x| {
                //                 x.map_err(|e| ParquetError::InvalidParameter(e.to_string()))
                //             }));

                //         // Page -> CompressedPage
                //         encoded_pages
                //             .map(|page| {
                //                 parquet2::write::compress(page?, Vec::new(), options.compression)
                //                     .map_err(|x| x.into())
                //             })
                //             .collect::<ArrowResult<VecDeque<CompressedPage>>>()
                //     })
                //     .collect::<Vec<ArrowResult<VecDeque<CompressedPage>>>>()

                let iter = std::iter::once(Ok(batch));

                iter.next().map(|maybe_chunk| {
                    let chunk = maybe_chunk?;
                    if self.encodings.len() != chunk.arrays().len() {
                        return Err(ArrowError::InvalidArgumentError(
                            "The number of arrays in the chunk must equal the number of fields in the schema"
                                .to_string(),
                        ));
                    };
                    let encodings = self.encodings.clone();
                    Ok(row_group_iter(
                        chunk,
                        (*encodings).clone(),
                        self.parquet_schema.fields().to_vec(),
                        options,
                    ))
                })
                        .collect::<Vec<ArrowResult<VecDeque<CompressedPage>>>>()
            })
            .collect::<ArrowResult<Vec<VecDeque<CompressedPage>>>>();

        let columns = columns?;

        // 将列级编码结果组装为一个 RowGroup：
        // RowGroupIter<'a, E> = DynIter<'a, Result<DynStreamingIterator<'a, CompressedPage, E>, E>>
        let row_group = DynIter::new(
            columns
                .into_iter()
                .map(|column| Ok(DynStreamingIterator::new(Bla::new(column)))),
        );

        // 交给 parquet2::write::FileWriter 顺序写入该 RowGroup
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
