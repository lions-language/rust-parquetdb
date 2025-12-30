use std::alloc::{Layout, alloc, dealloc, realloc};
use std::fs::OpenOptions;
use std::io::{self, Write};
use std::ptr::{self, NonNull};
use std::slice;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow2::{
    array::Array,
    chunk::Chunk,
    datatypes::Schema,
    io::parquet::write::{
        CompressionOptions, Encoding, RowGroupIterator, Version, WriteOptions, to_parquet_schema,
    },
};
use parquet2::metadata::SchemaDescriptor;
use std::os::unix::fs::OpenOptionsExt;

const ALIGN: usize = 4096;
const CHUNK: usize = 1 << 20; // 1 MiB
/// Linux 上 O_DIRECT 的值，一般为 0o40000。
/// 这里直接按约定常量定义，避免额外依赖 libc。
const O_DIRECT: i32 = 0o40000;

#[inline]
fn align_up(x: usize, align: usize) -> usize {
    (x + align - 1) & !(align - 1)
}

pub struct AlignedWriter {
    ptr: NonNull<u8>,
    len: usize,
    cap: usize,
}

impl AlignedWriter {
    pub fn new(capacity: usize) -> io::Result<Self> {
        let cap = align_up(capacity, ALIGN);
        let layout = Layout::from_size_align(cap, ALIGN)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "bad layout"))?;

        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "alloc failed"));
        }

        Ok(Self {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            len: 0,
            cap,
        })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl AlignedWriter {
    fn reserve(&mut self, additional: usize) -> io::Result<()> {
        let needed = self.len + additional;
        if needed <= self.cap {
            return Ok(());
        }

        let new_cap = align_up(self.cap.max(1) * 2, ALIGN).max(align_up(needed, ALIGN));

        let old_layout = Layout::from_size_align(self.cap, ALIGN).unwrap();
        let new_layout = Layout::from_size_align(new_cap, ALIGN).unwrap();

        let new_ptr = unsafe { realloc(self.ptr.as_ptr(), old_layout, new_layout.size()) };

        if new_ptr.is_null() {
            return Err(io::Error::new(io::ErrorKind::OutOfMemory, "realloc failed"));
        }

        self.ptr = unsafe { NonNull::new_unchecked(new_ptr) };
        self.cap = new_cap;
        Ok(())
    }
}

impl Write for AlignedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.reserve(buf.len())?;

        unsafe {
            ptr::copy_nonoverlapping(buf.as_ptr(), self.ptr.as_ptr().add(self.len), buf.len());
        }

        self.len += buf.len();
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// 对齐到 `ALIGN` 的缓冲区，用于 O_DIRECT 写入。
struct DirectIoBuffer {
    ptr: *mut u8,
    layout: Layout,
    len: usize,
}

impl DirectIoBuffer {
    fn new(size: usize) -> Result<Self> {
        let layout = Layout::from_size_align(size, ALIGN)
            .map_err(|e| anyhow!("invalid layout for direct I/O buffer: {e}"))?;

        // SAFETY: layout 来自 from_size_align，保证合法。
        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            Err(anyhow!("failed to allocate direct I/O buffer"))
        } else {
            Ok(Self {
                ptr,
                layout,
                len: size,
            })
        }
    }

    fn as_mut_slice(&mut self, len: usize) -> &mut [u8] {
        assert!(len <= self.len);
        // SAFETY: ptr 指向长度为 self.len 的有效分配，len <= self.len
        unsafe { std::slice::from_raw_parts_mut(self.ptr, len) }
    }
}

impl Drop for DirectIoBuffer {
    fn drop(&mut self) {
        // SAFETY: ptr 来自 alloc，layout 与之匹配；只在 Drop 中调用一次。
        unsafe {
            if !self.ptr.is_null() {
                dealloc(self.ptr, self.layout);
            }
        }
    }
}

/// 基于内存合并 + Direct I/O 落盘的 Parquet writer。
///
/// - 编码路径与 MemoryMergeParquetWriter 相同：使用 RowGroupIterator 将 batch
///   转为 RowGroup，写入到内存 Vec<u8> 后端的 FileWriter；
/// - 关闭时使用 O_DIRECT+对齐缓冲，将 Vec<u8> 按 4K 对齐的大块写入磁盘，
///   尾部通过零填充到对齐长度，再用 set_len 截断到真实大小。
/// - 不长期持有 File 句柄，只保存 path:String，便于与 Engine::flush 语义对齐。
pub struct DirectIoParquetWriter {
    schema: Arc<Schema>,
    parquet_schema: Arc<SchemaDescriptor>,
    options: Arc<WriteOptions>,
    encodings: Arc<Vec<Vec<Encoding>>>,
    writer: parquet2::write::FileWriter<Vec<u8>>,
    path: String,
}

impl super::ParquetWriter for DirectIoParquetWriter {
    fn try_new(path: &str, schema: Arc<Schema>) -> Result<Self> {
        let parquet_schema = to_parquet_schema(&*schema)?;

        let options = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Zstd(None),
            version: Version::V2,
            data_pagesize_limit: None,
        };

        // 每个 field 使用 Plain 编码，保持与现有 writer 一致
        let encodings: Vec<Vec<_>> = schema
            .fields
            .iter()
            .map(|_| vec![Encoding::Plain])
            .collect();

        let writer = parquet2::write::FileWriter::new(
            Vec::new(),
            parquet_schema.clone(),
            parquet2::write::WriteOptions {
                write_statistics: true,
                version: parquet2::write::Version::V2,
            },
            None,
        );

        Ok(Self {
            schema,
            parquet_schema: Arc::new(parquet_schema),
            options: Arc::new(options),
            encodings: Arc::new(encodings),
            writer,
            path: path.to_string(),
        })
    }

    fn write_batch(&mut self, batch: Chunk<Box<dyn Array>>) -> Result<()> {
        // 与 MemoryMergeParquetWriter 相同，使用 RowGroupIterator 负责编码与行组切分。
        let row_groups = RowGroupIterator::try_new(
            std::iter::once(Ok(batch)),
            &self.schema,
            (*self.options).clone(),
            (*self.encodings).clone(),
        )?;

        for row_group in row_groups {
            self.writer.write(row_group?)?;
        }

        Ok(())
    }

    fn close(self) -> Result<Self> {
        // 先结束 FileWriter，写入 footer 等元数据。
        let DirectIoParquetWriter {
            schema,
            parquet_schema,
            options,
            encodings,
            mut writer,
            path,
        } = self;

        writer.end(None)?;
        let buf = writer.into_inner();
        let len = buf.len();

        // 使用 O_DIRECT 打开文件并按对齐要求顺序写入。
        {
            // 注意：OpenOptions::write/create/truncate 已经设置了 O_WRONLY|O_CREAT|O_TRUNC，
            // custom_flags 再附加 O_DIRECT 即可。
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .custom_flags(O_DIRECT)
                .open(&path)?;

            if len > 0 {
                debug_assert_eq!(CHUNK % ALIGN, 0);
                let mut aligned = DirectIoBuffer::new(CHUNK)?;

                let mut offset = 0;
                while offset < len {
                    let remaining = len - offset;
                    let this_chunk = remaining.min(CHUNK);
                    // 写入长度也需满足对齐要求
                    let padded = ((this_chunk + ALIGN - 1) / ALIGN) * ALIGN;

                    let slice = aligned.as_mut_slice(padded);
                    // 拷贝真实数据
                    slice[..this_chunk].copy_from_slice(&buf[offset..offset + this_chunk]);
                    // 尾部补零到对齐长度，避免脏数据写入
                    if padded > this_chunk {
                        slice[this_chunk..padded].fill(0);
                    }

                    file.write_all(slice)?;
                    offset += this_chunk;
                }

                // 最后一块写入包含了零填充，通过 set_len 截断到真实长度，
                // 确保对上层可见的 Parquet 文件大小与内容完全正确。
                file.set_len(len as u64)?;
            } else {
                // 空文件的场景，保证文件存在且长度为 0。
                file.set_len(0)?;
            }
        }

        // 为保持 Engine::flush 语义，使用已经写入过的 Vec<u8> 重新构造 FileWriter，
        // 后续写入会继续在该 Vec 末尾追加。
        let writer = parquet2::write::FileWriter::new(
            buf,
            (*parquet_schema).clone(),
            parquet2::write::WriteOptions {
                write_statistics: true,
                version: parquet2::write::Version::V2,
            },
            None,
        );

        Ok(Self {
            schema,
            parquet_schema,
            options,
            encodings,
            writer,
            path,
        })
    }
}
