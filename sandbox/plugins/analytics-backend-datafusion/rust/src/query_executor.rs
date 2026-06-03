/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use arrow::datatypes::SchemaRef;
use datafusion::{
    common::DataFusionError,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    execution::context::SessionContext,
    execution::runtime_env::RuntimeEnvBuilder,
    execution::SessionStateBuilder,
    physical_plan::execute_stream,
    prelude::*,
};
use datafusion_common::config::EncryptionFactoryOptions;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::{CacheAccessor, DefaultListFilesCache};
use datafusion_execution::parquet_encryption::EncryptionFactory;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use log::error;
use object_store::ObjectMeta;
use object_store::path::Path;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use prost::Message;
use substrait::proto::Plan;

use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::api::DataFusionRuntime;

pub const OPENSEARCH_PME_FACTORY_ID: &str = "opensearch_pme";

#[derive(Debug)]
pub struct OpenSearchPmeDecryptionFactory {
    file_footer_keys: Arc<HashMap<String, Vec<u8>>>,
    file_aad_prefixes: Arc<HashMap<String, Vec<u8>>>,
}

impl OpenSearchPmeDecryptionFactory {
    pub fn new(file_footer_keys: Arc<HashMap<String, Vec<u8>>>, file_aad_prefixes: Arc<HashMap<String, Vec<u8>>>) -> Self {
        Self { file_footer_keys, file_aad_prefixes }
    }
}

#[async_trait]
impl EncryptionFactory for OpenSearchPmeDecryptionFactory {
    async fn get_file_encryption_properties(
        &self,
        _config: &EncryptionFactoryOptions,
        _schema: &SchemaRef,
        _file_path: &Path,
    ) -> datafusion_common::Result<Option<Arc<FileEncryptionProperties>>> {
        Ok(None)
    }

    async fn get_file_decryption_properties(
        &self,
        _config: &EncryptionFactoryOptions,
        file_path: &Path,
    ) -> datafusion_common::Result<Option<Arc<FileDecryptionProperties>>> {
        let filename = match file_path.filename() {
            Some(f) => f,
            None => return Ok(None),
        };
        match self.file_footer_keys.get(filename) {
            Some(footer_key) => {
                let mut builder = FileDecryptionProperties::builder(footer_key.clone());
                // Set the AAD prefix if present — required for files written with an AAD prefix.
                if let Some(aad_prefix) = self.file_aad_prefixes.get(filename) {
                    if !aad_prefix.is_empty() {
                        builder = builder.with_aad_prefix(aad_prefix.clone());
                    }
                }
                let properties = builder
                    .build()
                    .map_err(|e| DataFusionError::Execution(format!("Failed to build PME decryption properties: {}", e)))?;
                Ok(Some(properties))
            }
            None => Ok(None),
        }
    }
}

/// Execute a vanilla parquet query: substrait plan → DataFusion → CrossRtStream.
/// File access goes through DataFusion's registered object store.
pub async fn execute_query(
    table_path: ListingTableUrl,
    object_metas: Arc<Vec<ObjectMeta>>,
    table_name: String,
    plan_bytes: Vec<u8>,
    file_footer_keys: Arc<HashMap<String, Vec<u8>>>,
    file_aad_prefixes: Arc<HashMap<String, Vec<u8>>>,
    runtime: &DataFusionRuntime,
    cpu_executor: DedicatedExecutor,
) -> Result<i64, DataFusionError> {
    // Pre-populate the list-files cache so DataFusion doesn't re-list the directory
    let list_file_cache = Arc::new(DefaultListFilesCache::default());
    let table_scoped_path = datafusion::execution::cache::TableScopedPath {
        table: None,
        path: table_path.prefix().clone(),
    };
    list_file_cache.put(&table_scoped_path, object_metas);

    // Build a per-query RuntimeEnv sharing the global memory pool + caches,
    // but with a fresh list-files cache for this query's shard files.
    let runtime_env = RuntimeEnvBuilder::from_runtime_env(&runtime.runtime_env)
        .with_cache_manager(
            CacheManagerConfig::default()
                .with_list_files_cache(Some(list_file_cache))
                .with_file_metadata_cache(Some(
                    runtime.runtime_env.cache_manager.get_file_metadata_cache(),
                ))
                .with_files_statistics_cache(
                    runtime.runtime_env.cache_manager.get_file_statistic_cache(),
                ),
        )
        .build()
        .map_err(|e| {
            error!("Failed to build runtime env: {}", e);
            e
        })?;

    if file_footer_keys.is_empty() == false {
        runtime_env.register_parquet_encryption_factory(
            OPENSEARCH_PME_FACTORY_ID,
            Arc::new(OpenSearchPmeDecryptionFactory::new(Arc::clone(&file_footer_keys), Arc::clone(&file_aad_prefixes))),
        );
    }

    // Build a fresh session state per query. TODO : Tune this during planning per query
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = false;
    config.options_mut().execution.target_partitions = 4;
    config.options_mut().execution.batch_size = 8192;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::from(runtime_env))
        .with_default_features()
        .build();

    let ctx = SessionContext::new_with_state(state);

    // Register table via ListingTable — all IO goes through object store.
    // If PME footer keys are present the factory is registered above and
    // ParquetFormat must reference it via factory_id so that
    // get_file_decryption_properties returns the right properties at scan time.
    let file_format = if file_footer_keys.is_empty() == false {
        use datafusion_common::config::TableParquetOptions;
        let mut parquet_options = TableParquetOptions::default();
        parquet_options.crypto.factory_id = Some(OPENSEARCH_PME_FACTORY_ID.to_owned());
        ParquetFormat::new().with_options(parquet_options)
    } else {
        ParquetFormat::new()
    };
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet")
        .with_collect_stat(true);

    let resolved_schema = listing_options
        .infer_schema(&ctx.state(), &table_path)
        .await
        .map_err(|e| {
            error!("Failed to infer schema: {}", e);
            e
        })?;

    let table_config = ListingTableConfig::new(table_path)
        .with_listing_options(listing_options)
        .with_schema(resolved_schema);

    let provider = Arc::new(ListingTable::try_new(table_config).map_err(|e| {
        error!("Failed to create listing table: {}", e);
        e
    })?);

    ctx.register_table(&table_name, provider).map_err(|e| {
        error!("Failed to register table: {}", e);
        e
    })?;

    // Decode substrait → logical plan → physical plan → stream
    let substrait_plan = Plan::decode(plan_bytes.as_slice()).map_err(|e| {
        DataFusionError::Execution(format!("Failed to decode Substrait: {}", e))
    })?;

    let logical_plan = from_substrait_plan(&ctx.state(), &substrait_plan).await?;
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;

    let df_stream = execute_stream(physical_plan, ctx.task_ctx()).map_err(|e| {
        error!("Failed to create execution stream: {}", e);
        e
    })?;

    // Wrap in CrossRtStream — CPU work runs on DedicatedExecutor
    let cross_rt_stream =
        CrossRtStream::new_with_df_error_stream(df_stream, cpu_executor);
    let wrapped = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
        cross_rt_stream.schema(),
        cross_rt_stream,
    );

    Ok(Box::into_raw(Box::new(wrapped)) as i64)
}
