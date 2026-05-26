/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for the Parquet writer.
//!
//! Return convention: `>= 0` success, `< 0` error pointer (negate to get ptr,
//! call `native_error_message`/`native_error_free`).

use std::slice;
use std::str;

use native_bridge_common::ffm_safe;

use crate::writer::{NativeParquetWriter, ParquetEncryptionOptions};

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null string pointer".to_string());
    }
    if len < 0 {
        return Err(format!("negative string length: {}", len));
    }
    let bytes = slice::from_raw_parts(ptr, len as usize);
    str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8: {}", e))
}

unsafe fn optional_bytes_from_raw(ptr: *const u8, len: i64) -> Result<Option<Vec<u8>>, String> {
    if ptr.is_null() {
        return Ok(None);
    }
    if len < 0 {
        return Err(format!("negative byte length: {}", len));
    }
    if len == 0 {
        return Ok(Some(Vec::new()));
    }
    let bytes = slice::from_raw_parts(ptr, len as usize);
    Ok(Some(bytes.to_vec()))
}

unsafe fn required_bytes_from_raw(ptr: *const u8, len: i64, field: &str) -> Result<Vec<u8>, String> {
    if ptr.is_null() {
        return Err(format!("{}: null pointer", field));
    }
    if len < 0 {
        return Err(format!("{}: negative length {}", field, len));
    }
    if len == 0 {
        return Ok(Vec::new());
    }
    let bytes = slice::from_raw_parts(ptr, len as usize);
    Ok(bytes.to_vec())
}

/// Creates a native Parquet writer.
///
/// Encryption is activated when `footer_key_ptr` is non-null. All three encryption fields
/// (`footer_key`, `key_metadata_json`, `aad_prefix`) must be provided together.
/// Pass null pointers and zero lengths for an unencrypted file.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_create_writer(
    file_ptr: *const u8,
    file_len: i64,
    schema_address: i64,
    footer_key_ptr: *const u8,
    footer_key_len: i64,
    key_metadata_json_ptr: *const u8,
    key_metadata_json_len: i64,
    aad_prefix_ptr: *const u8,
    aad_prefix_len: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_create_writer: file: {}", e))?.to_string();

    let encryption_options = if footer_key_ptr.is_null() {
        None
    } else {
        let footer_key = required_bytes_from_raw(footer_key_ptr, footer_key_len, "footer_key")
            .map_err(|e| format!("parquet_create_writer: {}", e))?;
        let key_metadata_json = required_bytes_from_raw(
            key_metadata_json_ptr, key_metadata_json_len, "key_metadata_json")
            .map_err(|e| format!("parquet_create_writer: {}", e))?;
        let aad_prefix = optional_bytes_from_raw(aad_prefix_ptr, aad_prefix_len)
            .map_err(|e| format!("parquet_create_writer: aad_prefix: {}", e))?
            .unwrap_or_default();
        Some(ParquetEncryptionOptions { footer_key, key_metadata_json, aad_prefix })
    };

    NativeParquetWriter::create_writer(filename, schema_address, encryption_options)
        .map(|_| 0)
        .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_write(
    file_ptr: *const u8,
    file_len: i64,
    array_address: i64,
    schema_address: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_write: {}", e))?.to_string();
    NativeParquetWriter::write_data(filename, array_address, schema_address)
        .map(|_| 0)
        .map_err(|e| e.to_string())
}

/// Returns 0 with metadata in out-pointers, 1 if no writer found.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_finalize_writer(
    file_ptr: *const u8,
    file_len: i64,
    version_out: *mut i32,
    num_rows_out: *mut i64,
    created_by_buf: *mut u8,
    created_by_buf_len: i64,
    created_by_len_out: *mut i64,
    crc32_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_finalize_writer: {}", e))?.to_string();
    match NativeParquetWriter::finalize_writer(filename) {
        Ok(Some(result)) => {
            let fm = result.metadata.file_metadata();
            if !version_out.is_null() { *version_out = fm.version(); }
            if !num_rows_out.is_null() { *num_rows_out = fm.num_rows(); }
            if let Some(cb) = fm.created_by() {
                if !created_by_buf.is_null() && created_by_buf_len > 0 {
                    let bytes = cb.as_bytes();
                    let n = bytes.len().min(created_by_buf_len as usize);
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), created_by_buf, n);
                    if !created_by_len_out.is_null() { *created_by_len_out = n as i64; }
                }
            } else if !created_by_len_out.is_null() {
                *created_by_len_out = -1;
            }
            if !crc32_out.is_null() { *crc32_out = result.crc32 as i64; }
            Ok(0)
        }
        Ok(None) => Ok(1),
        Err(e) => Err(e.to_string()),
    }
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_sync_to_disk(
    file_ptr: *const u8,
    file_len: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_sync_to_disk: {}", e))?.to_string();
    NativeParquetWriter::sync_to_disk(filename)
        .map(|_| 0)
        .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_file_metadata(
    file_ptr: *const u8,
    file_len: i64,
    version_out: *mut i32,
    num_rows_out: *mut i64,
    created_by_buf: *mut u8,
    created_by_buf_len: i64,
    created_by_len_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_get_file_metadata: {}", e))?.to_string();
    let fm = NativeParquetWriter::get_file_metadata(filename).map_err(|e| e.to_string())?;
    if !version_out.is_null() { *version_out = fm.version(); }
    if !num_rows_out.is_null() { *num_rows_out = fm.num_rows(); }
    if let Some(cb) = fm.created_by() {
        if !created_by_buf.is_null() && created_by_buf_len > 0 {
            let bytes = cb.as_bytes();
            let n = bytes.len().min(created_by_buf_len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), created_by_buf, n);
            if !created_by_len_out.is_null() { *created_by_len_out = n as i64; }
        }
    } else if !created_by_len_out.is_null() {
        *created_by_len_out = -1;
    }
    Ok(0)
}

/// Reads encrypted Parquet file metadata.
/// footer_key and aad_prefix are required when non-null.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_file_metadata_decrypted(
    file_ptr: *const u8,
    file_len: i64,
    footer_key_ptr: *const u8,
    footer_key_len: i64,
    aad_prefix_ptr: *const u8,
    aad_prefix_len: i64,
    version_out: *mut i32,
    num_rows_out: *mut i64,
    created_by_buf: *mut u8,
    created_by_buf_len: i64,
    created_by_len_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_get_file_metadata_decrypted: {}", e))?.to_string();
    let footer_key = required_bytes_from_raw(footer_key_ptr, footer_key_len, "footer_key")
        .map_err(|e| format!("parquet_get_file_metadata_decrypted: {}", e))?;
    if footer_key.is_empty() {
        return Err("parquet_get_file_metadata_decrypted: footer_key must not be empty".to_string());
    }
    let aad_prefix = optional_bytes_from_raw(aad_prefix_ptr, aad_prefix_len)
        .map_err(|e| format!("parquet_get_file_metadata_decrypted: aad_prefix: {}", e))?;

    let fm = NativeParquetWriter::get_file_metadata_decrypted(filename, footer_key, aad_prefix)
        .map_err(|e| e.to_string())?;
    if !version_out.is_null() { *version_out = fm.version(); }
    if !num_rows_out.is_null() { *num_rows_out = fm.num_rows(); }
    if let Some(cb) = fm.created_by() {
        if !created_by_buf.is_null() && created_by_buf_len > 0 {
            let bytes = cb.as_bytes();
            let n = bytes.len().min(created_by_buf_len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), created_by_buf, n);
            if !created_by_len_out.is_null() { *created_by_len_out = n as i64; }
        }
    } else if !created_by_len_out.is_null() {
        *created_by_len_out = -1;
    }
    Ok(0)
}

/// Returns decrypted row count for an encrypted Parquet file.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_decrypted_num_rows(
    file_ptr: *const u8,
    file_len: i64,
    footer_key_ptr: *const u8,
    footer_key_len: i64,
    aad_prefix_ptr: *const u8,
    aad_prefix_len: i64,
    num_rows_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_get_decrypted_num_rows: {}", e))?.to_string();
    let footer_key = required_bytes_from_raw(footer_key_ptr, footer_key_len, "footer_key")
        .map_err(|e| format!("parquet_get_decrypted_num_rows: {}", e))?;
    if footer_key.is_empty() {
        return Err("parquet_get_decrypted_num_rows: footer_key must not be empty".to_string());
    }
    let aad_prefix = optional_bytes_from_raw(aad_prefix_ptr, aad_prefix_len)
        .map_err(|e| format!("parquet_get_decrypted_num_rows: aad_prefix: {}", e))?;

    let num_rows = NativeParquetWriter::get_decrypted_num_rows(filename, footer_key, aad_prefix)
        .map_err(|e| e.to_string())?;
    if !num_rows_out.is_null() {
        *num_rows_out = num_rows;
    }
    Ok(0)
}

/// Reads the plaintext `key_metadata` bytes from `FileCryptoMetaData` without decrypting.
///
/// Returns 0 and writes bytes to `out_buf` if key_metadata is present.
/// Returns 1 if the file is not encrypted or has no key_metadata (out_buf untouched).
/// Returns negative error code on failure.
#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_read_key_metadata(
    file_ptr: *const u8,
    file_len: i64,
    out_buf: *mut u8,
    out_buf_len: i64,
    out_len_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_read_key_metadata: {}", e))?.to_string();

    match NativeParquetWriter::read_key_metadata(filename).map_err(|e| e.to_string())? {
        None => Ok(1), // Not encrypted or no key_metadata
        Some(key_metadata) => {
            let n = key_metadata.len();
            if !out_buf.is_null() && out_buf_len > 0 {
                let copy_len = n.min(out_buf_len as usize);
                std::ptr::copy_nonoverlapping(key_metadata.as_ptr(), out_buf, copy_len);
                if !out_len_out.is_null() {
                    *out_len_out = copy_len as i64;
                }
            } else if !out_len_out.is_null() {
                *out_len_out = n as i64;
            }
            Ok(0)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn parquet_get_filtered_native_bytes_used(
    prefix_ptr: *const u8,
    prefix_len: i64,
) -> i64 {
    let prefix = str_from_raw(prefix_ptr, prefix_len).unwrap_or("").to_string();
    NativeParquetWriter::get_filtered_writer_memory_usage(prefix).unwrap_or(0) as i64
}
