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

unsafe fn optional_str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<Option<&'a str>, String> {
    if ptr.is_null() {
        return Ok(None);
    }
    if len < 0 {
        return Err(format!("negative string length: {}", len));
    }
    if len == 0 {
        return Ok(Some(""));
    }
    str_from_raw(ptr, len).map(Some)
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

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_create_writer(
    file_ptr: *const u8,
    file_len: i64,
    schema_address: i64,
    kms_instance_id_ptr: *const u8,
    kms_instance_id_len: i64,
    kms_instance_type_ptr: *const u8,
    kms_instance_type_len: i64,
    kms_key_arn_ptr: *const u8,
    kms_key_arn_len: i64,
    kms_encryption_context_ptr: *const u8,
    kms_encryption_context_len: i64,
    footer_key_ptr: *const u8,
    footer_key_len: i64,
    wrapped_footer_key_ptr: *const u8,
    wrapped_footer_key_len: i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len).map_err(|e| format!("parquet_create_writer: {}", e))?.to_string();
    // Prototyp: Wir aktivieren PME nur, wenn alle KMS-Felder + Footer-Key vorhanden sind.
    let encryption_options = match (
        optional_str_from_raw(kms_instance_id_ptr, kms_instance_id_len),
        optional_str_from_raw(kms_instance_type_ptr, kms_instance_type_len),
        optional_str_from_raw(kms_key_arn_ptr, kms_key_arn_len),
        optional_str_from_raw(kms_encryption_context_ptr, kms_encryption_context_len),
        optional_bytes_from_raw(footer_key_ptr, footer_key_len),
        optional_bytes_from_raw(wrapped_footer_key_ptr, wrapped_footer_key_len),
    ) {
        (
            Ok(Some(kms_instance_id)),
            Ok(Some(kms_instance_type)),
            Ok(Some(kms_key_arn)),
            Ok(Some(kms_encryption_context)),
            Ok(Some(footer_key)),
            Ok(Some(wrapped_footer_key)),
        ) => {
            Some(ParquetEncryptionOptions {
                kms_instance_id: kms_instance_id.to_string(),
                kms_instance_type: kms_instance_type.to_string(),
                kms_key_arn: kms_key_arn.to_string(),
                kms_encryption_context: kms_encryption_context.to_string(),
                footer_key,
                wrapped_footer_key,
            })
        }
        (Ok(None), Ok(None), Ok(None), Ok(None), Ok(None), Ok(None)) => None,
        (id, ty, arn, ctx, key, wrapped_key) => {
            let mut errors: Vec<String> = Vec::new();
            if let Err(e) = id {
                errors.push(format!("kms_instance_id: {}", e));
            }
            if let Err(e) = ty {
                errors.push(format!("kms_instance_type: {}", e));
            }
            if let Err(e) = arn {
                errors.push(format!("kms_key_arn: {}", e));
            }
            if let Err(e) = ctx {
                errors.push(format!("kms_encryption_context: {}", e));
            }
            if let Err(e) = key {
                errors.push(format!("footer_key: {}", e));
            }
            if let Err(e) = wrapped_key {
                errors.push(format!("wrapped_footer_key: {}", e));
            }
            if errors.is_empty() {
                errors.push(
                    "incomplete encryption payload: all encryption fields must be set together".to_string(),
                );
            }
            return Err(format!("parquet_create_writer: invalid encryption payload: {}", errors.join(", ")));
        }
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

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_file_metadata_decrypted(
    file_ptr: *const u8,
    file_len: i64,
    footer_key_ptr: *const u8,
    footer_key_len: i64,
    version_out: *mut i32,
    num_rows_out: *mut i64,
    created_by_buf: *mut u8,
    created_by_buf_len: i64,
    created_by_len_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_get_file_metadata_decrypted: {}", e))?
        .to_string();
    let footer_key = optional_bytes_from_raw(footer_key_ptr, footer_key_len)
        .map_err(|e| format!("parquet_get_file_metadata_decrypted: footer_key: {}", e))?
        .ok_or_else(|| "parquet_get_file_metadata_decrypted: missing footer_key".to_string())?;
    if footer_key.is_empty() {
        return Err("parquet_get_file_metadata_decrypted: footer_key must not be empty".to_string());
    }
    let fm = NativeParquetWriter::get_file_metadata_decrypted(filename, footer_key).map_err(|e| e.to_string())?;
    if !version_out.is_null() {
        *version_out = fm.version();
    }
    if !num_rows_out.is_null() {
        *num_rows_out = fm.num_rows();
    }
    if let Some(cb) = fm.created_by() {
        if !created_by_buf.is_null() && created_by_buf_len > 0 {
            let bytes = cb.as_bytes();
            let n = bytes.len().min(created_by_buf_len as usize);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), created_by_buf, n);
            if !created_by_len_out.is_null() {
                *created_by_len_out = n as i64;
            }
        }
    } else if !created_by_len_out.is_null() {
        *created_by_len_out = -1;
    }
    Ok(0)
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn parquet_get_decrypted_num_rows(
    file_ptr: *const u8,
    file_len: i64,
    footer_key_ptr: *const u8,
    footer_key_len: i64,
    num_rows_out: *mut i64,
) -> i64 {
    let filename = str_from_raw(file_ptr, file_len)
        .map_err(|e| format!("parquet_get_decrypted_num_rows: {}", e))?
        .to_string();
    let footer_key = optional_bytes_from_raw(footer_key_ptr, footer_key_len)
        .map_err(|e| format!("parquet_get_decrypted_num_rows: footer_key: {}", e))?
        .ok_or_else(|| "parquet_get_decrypted_num_rows: missing footer_key".to_string())?;
    if footer_key.is_empty() {
        return Err("parquet_get_decrypted_num_rows: footer_key must not be empty".to_string());
    }
    let num_rows = NativeParquetWriter::get_decrypted_num_rows(filename, footer_key).map_err(|e| e.to_string())?;
    if !num_rows_out.is_null() {
        *num_rows_out = num_rows;
    }
    Ok(0)
}

#[no_mangle]
pub unsafe extern "C" fn parquet_get_filtered_native_bytes_used(
    prefix_ptr: *const u8,
    prefix_len: i64,
) -> i64 {
    let prefix = str_from_raw(prefix_ptr, prefix_len).unwrap_or("").to_string();
    NativeParquetWriter::get_filtered_writer_memory_usage(prefix).unwrap_or(0) as i64
}
