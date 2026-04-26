/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! FFM bridge for DataFusion.

use std::slice;
use std::str;
use std::sync::Arc;
use std::collections::HashMap;

use native_bridge_common::ffm_safe;
use parking_lot::RwLock;

use crate::api;
use crate::runtime_manager::RuntimeManager;

static TOKIO_RUNTIME_MANAGER: RwLock<Option<Arc<RuntimeManager>>> = RwLock::new(None);

unsafe fn bytes_from_raw<'a>(ptr: *const u8, len: i64, arg_name: &str) -> Result<&'a [u8], String> {
    if len < 0 {
        return Err(format!("negative {} length: {}", arg_name, len));
    }
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(format!("null {} pointer", arg_name));
    }
    Ok(slice::from_raw_parts(ptr, len as usize))
}

unsafe fn ptr_array_from_raw<'a, T>(ptr: *const T, count: i64, arg_name: &str) -> Result<&'a [T], String> {
    if count < 0 {
        return Err(format!("negative {} count: {}", arg_name, count));
    }
    if count == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err(format!("null {} pointer", arg_name));
    }
    Ok(slice::from_raw_parts(ptr, count as usize))
}

unsafe fn str_from_raw<'a>(ptr: *const u8, len: i64) -> Result<&'a str, String> {
    str_from_raw_named(ptr, len, "string")
}

unsafe fn str_from_raw_named<'a>(ptr: *const u8, len: i64, arg_name: &str) -> Result<&'a str, String> {
    let bytes = bytes_from_raw(ptr, len, arg_name)?;
    str::from_utf8(bytes).map_err(|e| format!("invalid UTF-8 for {}: {}", arg_name, e))
}

unsafe fn string_array_from_raw(
    ptrs: *const *const u8,
    lens: *const i64,
    count: i64,
    arg_name: &str,
) -> Result<Vec<String>, String> {
    let ptrs = ptr_array_from_raw(ptrs, count, &format!("{} pointers", arg_name))?;
    let lens = ptr_array_from_raw(lens, count, &format!("{} lengths", arg_name))?;
    let mut values = Vec::with_capacity(count.max(0) as usize);
    for (idx, (&ptr, &len)) in ptrs.iter().zip(lens.iter()).enumerate() {
        values.push(str_from_raw_named(ptr, len, &format!("{}[{}]", arg_name, idx))?.to_string());
    }
    Ok(values)
}

unsafe fn footer_keys_from_raw(
    key_files_ptr: *const *const u8,
    key_files_len_ptr: *const i64,
    key_files_count: i64,
    key_bytes_ptr: *const *const u8,
    key_bytes_len_ptr: *const i64,
    key_bytes_count: i64,
) -> Result<HashMap<String, Vec<u8>>, String> {
    if key_files_count != key_bytes_count {
        return Err("key file and key byte counts must match".to_string());
    }

    let key_files = string_array_from_raw(key_files_ptr, key_files_len_ptr, key_files_count, "key_files")?;
    let key_ptrs = ptr_array_from_raw(key_bytes_ptr, key_bytes_count, "key_bytes pointers")?;
    let key_lens = ptr_array_from_raw(key_bytes_len_ptr, key_bytes_count, "key_bytes lengths")?;
    let mut file_footer_keys = HashMap::with_capacity(key_files_count.max(0) as usize);

    for (idx, ((file_name, &key_ptr), &key_len)) in key_files
        .iter()
        .zip(key_ptrs.iter())
        .zip(key_lens.iter())
        .enumerate()
    {
        let key = bytes_from_raw(key_ptr, key_len, &format!("key_bytes[{}]", idx))?.to_vec();
        if key.is_empty() {
            return Err(format!("empty footer key for file {}", file_name));
        }
        file_footer_keys.insert(file_name.clone(), key);
    }

    Ok(file_footer_keys)
}

fn get_rt_manager() -> Result<Arc<RuntimeManager>, String> {
    TOKIO_RUNTIME_MANAGER
        .read()
        .clone()
        .ok_or_else(|| "Runtime manager not initialized".to_string())
}

#[no_mangle]
pub extern "C" fn df_init_runtime_manager(cpu_threads: i32) {
    let mut guard = TOKIO_RUNTIME_MANAGER.write();
    *guard = Some(Arc::new(RuntimeManager::new(cpu_threads as usize)));
}

#[no_mangle]
pub extern "C" fn df_shutdown_runtime_manager() {
    let mgr = TOKIO_RUNTIME_MANAGER.write().take();
    if let Some(mgr) = mgr {
        mgr.shutdown();
    }
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_global_runtime(
    memory_pool_limit: i64,
    spill_dir_ptr: *const u8,
    spill_dir_len: i64,
    spill_limit: i64,
) -> i64 {
    let spill_dir = str_from_raw(spill_dir_ptr, spill_dir_len).map_err(|e| format!("df_create_global_runtime: {}", e))?;
    api::create_global_runtime(memory_pool_limit, spill_dir, spill_limit)
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_global_runtime(ptr: i64) {
    api::close_global_runtime(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_create_reader(
    table_path_ptr: *const u8,
    table_path_len: i64,
    files_ptr: *const *const u8,
    files_len_ptr: *const i64,
    files_count: i64,
    key_files_ptr: *const *const u8,
    key_files_len_ptr: *const i64,
    key_files_count: i64,
    key_bytes_ptr: *const *const u8,
    key_bytes_len_ptr: *const i64,
    key_bytes_count: i64,
) -> i64 {
    let table_path = str_from_raw(table_path_ptr, table_path_len).map_err(|e| format!("df_create_reader: {}", e))?;
    let filenames = string_array_from_raw(files_ptr, files_len_ptr, files_count, "files")
        .map_err(|e| format!("df_create_reader: {}", e))?;
    let file_footer_keys = footer_keys_from_raw(
        key_files_ptr,
        key_files_len_ptr,
        key_files_count,
        key_bytes_ptr,
        key_bytes_len_ptr,
        key_bytes_count,
    )
    .map_err(|e| format!("df_create_reader: {}", e))?;
    let mgr = get_rt_manager()?;
    api::create_reader(table_path, filenames, file_footer_keys, &mgr).map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_close_reader(ptr: i64) {
    api::close_reader(ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_execute_query(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    plan_ptr: *const u8,
    plan_len: i64,
    runtime_ptr: i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len).map_err(|e| format!("df_execute_query: {}", e))?;
    let plan_bytes = bytes_from_raw(plan_ptr, plan_len, "plan").map_err(|e| format!("df_execute_query: {}", e))?;
    mgr.io_runtime
        .block_on(api::execute_query(shard_view_ptr, table_name, plan_bytes, runtime_ptr, &mgr))
        .map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stream_get_schema(stream_ptr: i64) -> i64 {
    api::stream_get_schema(stream_ptr).map_err(|e| e.to_string())
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_stream_next(stream_ptr: i64) -> i64 {
    let mgr = get_rt_manager()?;
    mgr.io_runtime
        .block_on(api::stream_next(stream_ptr))
        .map_err(|e| e.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn df_stream_close(stream_ptr: i64) {
    api::stream_close(stream_ptr);
}

#[ffm_safe]
#[no_mangle]
pub unsafe extern "C" fn df_sql_to_substrait(
    shard_view_ptr: i64,
    table_name_ptr: *const u8,
    table_name_len: i64,
    sql_ptr: *const u8,
    sql_len: i64,
    runtime_ptr: i64,
    out_ptr: *mut u8,
    out_cap: i64,
    out_len: *mut i64,
) -> i64 {
    let mgr = get_rt_manager()?;
    let table_name = str_from_raw(table_name_ptr, table_name_len).map_err(|e| format!("df_sql_to_substrait: table_name: {}", e))?;
    let sql = str_from_raw(sql_ptr, sql_len).map_err(|e| format!("df_sql_to_substrait: sql: {}", e))?;
    if out_cap < 0 {
        return Err(format!("df_sql_to_substrait: negative output capacity: {}", out_cap));
    }
    if out_cap > 0 && out_ptr.is_null() {
        return Err("df_sql_to_substrait: null output buffer pointer".to_string());
    }
    let bytes = api::sql_to_substrait(shard_view_ptr, table_name, sql, runtime_ptr, &mgr)
        .map_err(|e| e.to_string())?;
    if bytes.len() > out_cap as usize {
        return Err(format!(
            "substrait plan size {} exceeds buffer capacity {}",
            bytes.len(),
            out_cap
        ));
    }
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), out_ptr, bytes.len());
    if !out_len.is_null() {
        *out_len = bytes.len() as i64;
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_from_raw_rejects_negative_length() {
        let err = unsafe { bytes_from_raw(std::ptr::null(), -1, "plan") }.unwrap_err();
        assert!(err.contains("negative plan length"));
    }

    #[test]
    fn test_string_array_from_raw_rejects_missing_lengths_pointer() {
        let value = b"file.parquet";
        let ptrs = [value.as_ptr()];
        let err = unsafe { string_array_from_raw(ptrs.as_ptr(), std::ptr::null(), 1, "files") }.unwrap_err();
        assert!(err.contains("null files lengths pointer"));
    }

    #[test]
    fn test_footer_keys_from_raw_rejects_count_mismatch() {
        let err = unsafe {
            footer_keys_from_raw(
                std::ptr::null(),
                std::ptr::null(),
                1,
                std::ptr::null(),
                std::ptr::null(),
                0,
            )
        }
        .unwrap_err();
        assert!(err.contains("counts must match"));
    }

    #[test]
    fn test_footer_keys_from_raw_rejects_empty_key() {
        let file = b"test.parquet";
        let key: [u8; 0] = [];
        let file_ptrs = [file.as_ptr()];
        let file_lens = [file.len() as i64];
        let key_ptrs = [key.as_ptr()];
        let key_lens = [0_i64];

        let err = unsafe {
            footer_keys_from_raw(
                file_ptrs.as_ptr(),
                file_lens.as_ptr(),
                1,
                key_ptrs.as_ptr(),
                key_lens.as_ptr(),
                1,
            )
        }
        .unwrap_err();
        assert!(err.contains("empty footer key for file test.parquet"));
    }

    #[test]
    fn test_footer_keys_from_raw_parses_valid_input() {
        let file = b"test.parquet";
        let key = [1_u8, 2, 3, 4];
        let file_ptrs = [file.as_ptr()];
        let file_lens = [file.len() as i64];
        let key_ptrs = [key.as_ptr()];
        let key_lens = [key.len() as i64];

        let keys = unsafe {
            footer_keys_from_raw(
                file_ptrs.as_ptr(),
                file_lens.as_ptr(),
                1,
                key_ptrs.as_ptr(),
                key_lens.as_ptr(),
                1,
            )
        }
        .unwrap();

        assert_eq!(keys.get("test.parquet"), Some(&key.to_vec()));
    }
}

