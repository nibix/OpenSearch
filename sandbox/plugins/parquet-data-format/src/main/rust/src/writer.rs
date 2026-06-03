/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use lazy_static::lazy_static;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use crate::{log_error, log_debug};

/// A write wrapper that computes CRC32 as bytes flow through.
pub struct Crc32Writer {
    inner: File,
    hasher: crc32fast::Hasher,
}

impl Crc32Writer {
    fn new(file: File) -> Self {
        Self {
            inner: file,
            hasher: crc32fast::Hasher::new(),
        }
    }

    fn checksum(&self) -> u32 {
        self.hasher.clone().finalize()
    }
}

impl Write for Crc32Writer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Result from finalizing a writer: Parquet metadata + whole-file CRC32.
#[derive(Debug)]
pub struct FinalizeResult {
    pub metadata: parquet::file::metadata::ParquetMetaData,
    pub crc32: u32,
}

lazy_static! {
    pub static ref WRITER_MANAGER: DashMap<String, Arc<Mutex<ArrowWriter<Crc32Writer>>>> = DashMap::new();
    pub static ref FILE_MANAGER: DashMap<String, File> = DashMap::new();
}

pub struct NativeParquetWriter;

/// Per-file PME encryption inputs passed from the Java bridge.
///
/// All fields are already fully derived by the Java side:
/// - `footer_key`: 32-byte derived AES-256 key (HMAC-SHA384 two-step derivation)
/// - `key_metadata_json`: v1 JSON bytes for `FileCryptoMetaData.key_metadata`
/// - `aad_prefix`: binary AAD prefix (domain + version + data_key_id + message_id)
pub struct ParquetEncryptionOptions {
    pub footer_key: Vec<u8>,
    pub key_metadata_json: Vec<u8>,
    pub aad_prefix: Vec<u8>,
}

impl NativeParquetWriter {
    pub fn create_writer(
        filename: String,
        schema_address: i64,
        encryption_options: Option<ParquetEncryptionOptions>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!("create_writer called for file: {}, schema_address: {}", filename, schema_address);

        if (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid schema address (null pointer) for file: {}", filename);
            return Err("Invalid schema address".into());
        }
        if WRITER_MANAGER.contains_key(&filename) {
            log_error!("ERROR: Writer already exists for file: {}", filename);
            return Err("Writer already exists for this file".into());
        }

        let arrow_schema = unsafe { FFI_ArrowSchema::from_raw(schema_address as *mut _) };
        let schema = Arc::new(arrow::datatypes::Schema::try_from(&arrow_schema)?);
        log_debug!("Schema created with {} fields", schema.fields().len());

        let file = File::create(&filename)?;
        let file_clone = file.try_clone()?;
        FILE_MANAGER.insert(filename.clone(), file_clone);

        let mut props_builder = WriterProperties::builder()
            .set_compression(Compression::LZ4_RAW)
            .set_bloom_filter_enabled(true)
            .set_bloom_filter_fpp(0.1)
            .set_bloom_filter_ndv(100000);

        if let Some(options) = encryption_options {
            // TODO at the moment, parquet-rs only supports 16 byte keys. This is in deviation of the intended target state.
            if options.footer_key.len() != 16 {
                return Err(format!(
                    "PME footer_key must be 16 bytes, got {}",
                    options.footer_key.len()
                ).into());
            }
            let mut enc_builder = FileEncryptionProperties::builder(options.footer_key)
                .with_footer_key_metadata(options.key_metadata_json);
            if options.aad_prefix.is_empty() == false {
                enc_builder = enc_builder.with_aad_prefix(options.aad_prefix);
            }
            let file_encryption_properties = enc_builder.build()?;
            props_builder = props_builder.with_file_encryption_properties(file_encryption_properties);
            log_debug!("PME enabled for file: {}", filename);
        }

        let props = props_builder.build();
        let crc_writer = Crc32Writer::new(file);
        let writer = ArrowWriter::try_new(crc_writer, schema, Some(props))?;
        WRITER_MANAGER.insert(filename, Arc::new(Mutex::new(writer)));
        Ok(())
    }

    pub fn write_data(filename: String, array_address: i64, schema_address: i64) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!("write_data called for file: {}", filename);

        if (array_address as *mut u8).is_null() || (schema_address as *mut u8).is_null() {
            log_error!("ERROR: Invalid FFI addresses for file: {}", filename);
            return Err("Invalid FFI addresses (null pointers)".into());
        }

        unsafe {
            let arrow_schema = FFI_ArrowSchema::from_raw(schema_address as *mut _);
            let arrow_array = FFI_ArrowArray::from_raw(array_address as *mut _);
            let array_data = arrow::ffi::from_ffi(arrow_array, &arrow_schema)?;
            let array: Arc<dyn arrow::array::Array> = arrow::array::make_array(array_data);

            if let Some(struct_array) = array.as_any().downcast_ref::<arrow::array::StructArray>() {
                let schema = Arc::new(arrow::datatypes::Schema::new(struct_array.fields().clone()));
                let record_batch = RecordBatch::try_new(schema, struct_array.columns().to_vec())?;
                log_debug!("Created RecordBatch with {} rows and {} columns", record_batch.num_rows(), record_batch.num_columns());

                if let Some(writer_arc) = WRITER_MANAGER.get(&filename) {
                    let mut writer = writer_arc.lock().unwrap();
                    writer.write(&record_batch)?;
                    Ok(())
                } else {
                    log_error!("ERROR: No writer found for file: {}", filename);
                    Err("Writer not found".into())
                }
            } else {
                log_error!("ERROR: Array is not a StructArray, type: {:?}", array.data_type());
                Err("Expected struct array from VectorSchemaRoot".into())
            }
        }
    }

    pub fn finalize_writer(filename: String) -> Result<Option<FinalizeResult>, Box<dyn std::error::Error>> {
        log_debug!("finalize_writer called for file: {}", filename);

        if let Some((_, writer_arc)) = WRITER_MANAGER.remove(&filename) {
            match Arc::try_unwrap(writer_arc) {
                Ok(mutex) => {
                    let mut writer = mutex.into_inner().unwrap();
                    let parquet_metadata = writer.finish()?;
                    let file_metadata = parquet_metadata.file_metadata();
                    log_debug!("Successfully finalized writer for file: {}, num_rows={}", filename, file_metadata.num_rows());
                    let crc32 = writer.inner().checksum();
                    log_debug!("CRC32 for file {}: {:#010x}", filename, crc32);
                    Ok(Some(FinalizeResult { metadata: parquet_metadata, crc32 }))
                }
                Err(_) => {
                    log_error!("ERROR: Writer still in use for file: {}", filename);
                    Err("Writer still in use".into())
                }
            }
        } else {
            log_error!("ERROR: Writer not found for file: {}", filename);
            Err("Writer not found".into())
        }
    }

    pub fn sync_to_disk(filename: String) -> Result<(), Box<dyn std::error::Error>> {
        log_debug!("sync_to_disk called for file: {}", filename);

        if let Some(file) = FILE_MANAGER.get_mut(&filename) {
            file.sync_all()?;
            log_debug!("Successfully fsynced file: {}", filename);
            drop(file);
            FILE_MANAGER.remove(&filename);
            Ok(())
        } else {
            log_error!("ERROR: File not found for fsync: {}", filename);
            Err("File not found".into())
        }
    }

    pub fn get_filtered_writer_memory_usage(path_prefix: String) -> Result<usize, Box<dyn std::error::Error>> {
        let mut total_memory = 0;
        for entry in WRITER_MANAGER.iter() {
            if entry.key().starts_with(&path_prefix) {
                if let Ok(writer) = entry.value().lock() {
                    total_memory += writer.memory_size();
                }
            }
        }
        Ok(total_memory)
    }

    pub fn get_file_metadata(filename: String) -> Result<parquet::file::metadata::FileMetaData, Box<dyn std::error::Error>> {
        let file = File::open(&filename)?;
        let reader = SerializedFileReader::new(file)?;
        let file_metadata = reader.metadata().file_metadata().clone();
        log_debug!("Metadata for {}: version={}, num_rows={}", filename, file_metadata.version(), file_metadata.num_rows());
        Ok(file_metadata)
    }

    /// Reads the encrypted Parquet file metadata using the provided footer key and AAD prefix.
    pub fn get_file_metadata_decrypted(
        filename: String,
        footer_key: Vec<u8>,
        aad_prefix: Option<Vec<u8>>,
    ) -> Result<parquet::file::metadata::FileMetaData, Box<dyn std::error::Error>> {
        let file = File::open(&filename)?;
        let mut dec_builder = FileDecryptionProperties::builder(footer_key);
        if let Some(aad) = aad_prefix {
            if aad.is_empty() == false {
                dec_builder = dec_builder.with_aad_prefix(aad);
            }
        }
        let decryption_properties = dec_builder.build()?;
        let options = ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);
        let metadata = ArrowReaderMetadata::load(&file, options)?;
        let file_metadata = metadata.metadata().file_metadata().clone();
        log_debug!(
            "Decrypted metadata for {}: version={}, num_rows={}",
            filename, file_metadata.version(), file_metadata.num_rows()
        );
        Ok(file_metadata)
    }

    /// Returns the decrypted row count using the provided footer key and AAD prefix.
    pub fn get_decrypted_num_rows(
        filename: String,
        footer_key: Vec<u8>,
        aad_prefix: Option<Vec<u8>>,
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let file = File::open(&filename)?;
        let mut dec_builder = FileDecryptionProperties::builder(footer_key);
        if let Some(aad) = aad_prefix {
            if aad.is_empty() == false {
                dec_builder = dec_builder.with_aad_prefix(aad);
            }
        }
        let decryption_properties = dec_builder.build()?;
        let options = ArrowReaderOptions::new().with_file_decryption_properties(decryption_properties);
        let mut reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?.build()?;
        let mut num_rows: i64 = 0;
        while let Some(batch) = reader.next() {
            num_rows += batch?.num_rows() as i64;
        }
        log_debug!("Decrypted payload rows for {}: {}", filename, num_rows);
        Ok(num_rows)
    }

    /// Reads the plaintext `key_metadata` bytes from an encrypted Parquet file's
    /// `FileCryptoMetaData` without decrypting the footer.
    ///
    /// Returns `None` if the file is not encrypted (magic "PAR1") or has no `key_metadata`.
    /// Returns `Some(bytes)` with the raw key_metadata bytes otherwise.
    pub fn read_key_metadata(filename: String) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
        let mut file = File::open(&filename)?;
        let file_size = file.seek(SeekFrom::End(0))?;
        if file_size < 8 {
            return Err(format!("File too small to be a valid Parquet file: {}", filename).into());
        }

        // Read last 8 bytes: [footer_or_crypto_meta_len: 4 le bytes] [magic: 4 bytes]
        file.seek(SeekFrom::End(-8))?;
        let mut tail = [0u8; 8];
        file.read_exact(&mut tail)?;

        let magic = &tail[4..8];
        if magic != b"PARE" {
            // Not an encrypted Parquet file — no key_metadata.
            return Ok(None);
        }

        // Encrypted file: last 8 bytes = [FileCryptoMetaData_len: i32 le][b"PARE"]
        let crypto_meta_len = i32::from_le_bytes([tail[0], tail[1], tail[2], tail[3]]);
        if crypto_meta_len <= 0 {
            return Err(format!("Invalid FileCryptoMetaData length {} in {}", crypto_meta_len, filename).into());
        }
        let crypto_meta_len = crypto_meta_len as u64;
        if file_size < 8 + crypto_meta_len {
            return Err(format!("FileCryptoMetaData length exceeds file size in {}", filename).into());
        }

        // Read FileCryptoMetaData Thrift compact bytes
        file.seek(SeekFrom::End(-(8 + crypto_meta_len as i64)))?;
        let mut crypto_meta_bytes = vec![0u8; crypto_meta_len as usize];
        file.read_exact(&mut crypto_meta_bytes)?;

        // Parse FileCryptoMetaData using parquet-rs Thrift types.
        // FileCryptoMetaData compact Thrift layout:
        //   field 1 (EncryptionAlgorithm, type struct): parse and skip
        //   field 2 (key_metadata, type binary, optional): if present, read bytes
        // We use a minimal hand-rolled parser to avoid thrift crate version coupling.
        parse_key_metadata_from_file_crypto_meta(&crypto_meta_bytes)
    }
}

/// Minimal Thrift compact protocol parser for `FileCryptoMetaData.key_metadata`.
///
/// We only need field 2 (binary, optional). We skip field 1 (EncryptionAlgorithm struct)
/// by recursively skipping the struct, then read field 2 if present.
fn parse_key_metadata_from_file_crypto_meta(bytes: &[u8]) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error>> {
    let mut pos = 0usize;

    // Thrift compact field header: (delta << 4) | type
    // Type codes: 1=bool_true, 2=bool_false, 3=i8, 4=i16, 5=i32, 6=i64, 7=double,
    //             8=binary, 9=list, 10=set, 11=map, 12=struct

    let mut last_field_id: i16 = 0;

    loop {
        if pos >= bytes.len() {
            break;
        }
        let byte = bytes[pos];
        pos += 1;

        if byte == 0x00 {
            // STOP field — end of struct
            break;
        }

        let delta = (byte >> 4) as i16;
        let type_id = byte & 0x0F;

        let field_id = if delta == 0 {
            // zig-zag encoded full field id follows
            let (fid, consumed) = read_zigzag_i16(&bytes[pos..])?;
            pos += consumed;
            fid
        } else {
            last_field_id + delta
        };
        last_field_id = field_id;

        match field_id {
            1 => {
                // EncryptionAlgorithm — type must be struct (0x0C)
                if type_id != 0x0C {
                    return Err(format!("Expected struct for field 1, got type {}", type_id).into());
                }
                pos = skip_thrift_struct(bytes, pos)?;
            }
            2 => {
                // key_metadata — type must be binary (0x08)
                if type_id != 0x08 {
                    return Err(format!("Expected binary for field 2, got type {}", type_id).into());
                }
                let (len, consumed) = read_varint_u64(&bytes[pos..])?;
                pos += consumed;
                let len = len as usize;
                if pos + len > bytes.len() {
                    return Err("key_metadata length exceeds buffer".into());
                }
                let key_metadata = bytes[pos..pos + len].to_vec();
                return Ok(Some(key_metadata));
            }
            _ => {
                // Skip unknown field
                pos = skip_thrift_field(bytes, pos, type_id)?;
            }
        }
    }

    Ok(None)
}

/// Skip a complete Thrift compact struct (including nested structs), returning new position.
fn skip_thrift_struct(bytes: &[u8], mut pos: usize) -> Result<usize, Box<dyn std::error::Error>> {
    let mut last_field_id: i16 = 0;
    loop {
        if pos >= bytes.len() {
            break;
        }
        let byte = bytes[pos];
        pos += 1;
        if byte == 0x00 {
            break; // STOP
        }
        let delta = (byte >> 4) as i16;
        let type_id = byte & 0x0F;
        let field_id = if delta == 0 {
            let (fid, consumed) = read_zigzag_i16(&bytes[pos..])?;
            pos += consumed;
            fid
        } else {
            last_field_id + delta
        };
        last_field_id = field_id;
        pos = skip_thrift_field(bytes, pos, type_id)?;
    }
    Ok(pos)
}

/// Skip a single Thrift compact field value of the given type, returning new position.
fn skip_thrift_field(bytes: &[u8], mut pos: usize, type_id: u8) -> Result<usize, Box<dyn std::error::Error>> {
    match type_id {
        0x01 | 0x02 => { /* bool: no extra bytes */ }
        0x03 => { pos += 1; } // i8: 1 byte
        0x04 | 0x05 | 0x06 => {
            // i16/i32/i64: zigzag varint
            let (_, consumed) = read_varint_u64(&bytes[pos..])?;
            pos += consumed;
        }
        0x07 => { pos += 8; } // double: 8 bytes
        0x08 => {
            // binary: varint length + bytes
            let (len, consumed) = read_varint_u64(&bytes[pos..])?;
            pos += consumed + len as usize;
        }
        0x09 | 0x0A => {
            // list/set: size_and_type byte, then elements
            if pos >= bytes.len() {
                return Err("Unexpected end of buffer in list/set".into());
            }
            let size_type = bytes[pos];
            pos += 1;
            let elem_type = size_type & 0x0F;
            let size = if (size_type >> 4) == 0x0F {
                let (s, consumed) = read_varint_u64(&bytes[pos..])?;
                pos += consumed;
                s as usize
            } else {
                (size_type >> 4) as usize
            };
            for _ in 0..size {
                pos = skip_thrift_field(bytes, pos, elem_type)?;
            }
        }
        0x0B => {
            // map: size varint, then key_type/val_type byte, then entries
            let (size, consumed) = read_varint_u64(&bytes[pos..])?;
            pos += consumed;
            if size > 0 {
                if pos >= bytes.len() {
                    return Err("Unexpected end of buffer in map".into());
                }
                let kv_type = bytes[pos];
                pos += 1;
                let key_type = (kv_type >> 4) & 0x0F;
                let val_type = kv_type & 0x0F;
                for _ in 0..size {
                    pos = skip_thrift_field(bytes, pos, key_type)?;
                    pos = skip_thrift_field(bytes, pos, val_type)?;
                }
            }
        }
        0x0C => {
            // nested struct: recurse
            pos = skip_thrift_struct(bytes, pos)?;
        }
        _ => {
            return Err(format!("Unknown Thrift compact type id: {}", type_id).into());
        }
    }
    Ok(pos)
}

fn read_varint_u64(bytes: &[u8]) -> Result<(u64, usize), Box<dyn std::error::Error>> {
    let mut value: u64 = 0;
    let mut shift = 0u32;
    let mut consumed = 0usize;
    for &b in bytes {
        consumed += 1;
        value |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok((value, consumed));
        }
        shift += 7;
        if shift >= 64 {
            return Err("Varint overflow".into());
        }
    }
    Err("Unexpected end of varint".into())
}

fn read_zigzag_i16(bytes: &[u8]) -> Result<(i16, usize), Box<dyn std::error::Error>> {
    let (n, consumed) = read_varint_u64(bytes)?;
    let zigzag = ((n >> 1) as i16) ^ (-((n & 1) as i16));
    Ok((zigzag, consumed))
}
