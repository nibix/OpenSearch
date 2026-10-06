/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Field-value masking at the raw shard-read boundary.
//!
//! The wrapper is installed only around providers that read parquet for a concrete shard.
//! Named-input and shuffle providers consume values already produced by such a scan and are
//! deliberately not wrapped, preventing a second masking pass.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayRef, LargeStringArray, StringArray, StringViewArray};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use blake2b_simd::Params as Blake2bParams;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::TableType;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::TryStreamExt;
use regex::Regex;
use sha2::{Digest, Sha256, Sha512};

#[derive(Clone)]
pub enum Transformation {
    Blake2bPersonalized(Vec<u8>),
    Blake2bSalted(Vec<u8>),
    Sha256,
    Sha512,
    RegexReplace(Vec<(Regex, String)>),
}

impl fmt::Debug for Transformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Blake2bPersonalized(_) => f.write_str("Blake2bPersonalized"),
            Self::Blake2bSalted(_) => f.write_str("Blake2bSalted"),
            Self::Sha256 => f.write_str("Sha256"),
            Self::Sha512 => f.write_str("Sha512"),
            Self::RegexReplace(rules) => f.debug_tuple("RegexReplace").field(&rules.len()).finish(),
        }
    }
}

pub type Transformations = Arc<HashMap<String, Transformation>>;

/// Parses the private, in-process Java/native descriptor encoding. It intentionally has no
/// transport version: descriptors are resolved and consumed on the same node and release.
pub fn decode(bytes: &[u8]) -> Result<Transformations> {
    if bytes.is_empty() {
        return Ok(Arc::new(HashMap::new()));
    }
    let mut cursor = Cursor { bytes, offset: 0 };
    let count = cursor.u32()? as usize;
    let mut result = HashMap::with_capacity(count);
    for _ in 0..count {
        let field = cursor.string()?;
        let kind = cursor.u8()?;
        let transformation = match kind {
            1 => {
                let algorithm = cursor.u8()?;
                let salt = cursor.byte_vec()?;
                match algorithm {
                    0 => Transformation::Blake2bPersonalized(salt),
                    1 => Transformation::Blake2bSalted(salt),
                    2 if salt.is_empty() => Transformation::Sha256,
                    3 if salt.is_empty() => Transformation::Sha512,
                    2 | 3 => {
                        return Err(DataFusionError::Execution(
                            "SHA field masking cannot carry a salt".into(),
                        ))
                    }
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "unknown field masking hash algorithm {algorithm}"
                        )))
                    }
                }
            }
            2 => {
                let rules = cursor.u32()? as usize;
                let mut replacements = Vec::with_capacity(rules);
                for _ in 0..rules {
                    let pattern = cursor.string()?;
                    let replacement = cursor.string()?;
                    replacements.push((
                        Regex::new(&pattern).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "invalid field masking regex [{pattern}]: {e}"
                            ))
                        })?,
                        replacement,
                    ));
                }
                Transformation::RegexReplace(replacements)
            }
            _ => {
                return Err(DataFusionError::Execution(format!(
                    "unknown field masking transformation kind {kind}"
                )))
            }
        };
        if result.insert(field.clone(), transformation).is_some() {
            return Err(DataFusionError::Execution(format!(
                "duplicate field masking descriptor for [{field}]"
            )));
        }
    }
    if cursor.offset != bytes.len() {
        return Err(DataFusionError::Execution(
            "trailing bytes in field masking descriptors".into(),
        ));
    }
    Ok(Arc::new(result))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl Cursor<'_> {
    fn take(&mut self, len: usize) -> Result<&[u8]> {
        let end = self.offset.checked_add(len).ok_or_else(|| {
            DataFusionError::Execution("field masking descriptor overflow".into())
        })?;
        if end > self.bytes.len() {
            return Err(DataFusionError::Execution(
                "truncated field masking descriptors".into(),
            ));
        }
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn byte_vec(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.byte_vec()?).map_err(|e| {
            DataFusionError::Execution(format!("field masking descriptor is not UTF-8: {e}"))
        })
    }
}

pub struct MaskingTableProvider {
    inner: Arc<dyn TableProvider>,
    transformations: Transformations,
}

impl MaskingTableProvider {
    pub fn wrap(
        inner: Arc<dyn TableProvider>,
        transformations: Transformations,
    ) -> Arc<dyn TableProvider> {
        if transformations.is_empty() {
            inner
        } else {
            Arc::new(Self {
                inner,
                transformations,
            })
        }
    }

    fn references_masked(&self, expr: &Expr) -> bool {
        expr.column_refs()
            .iter()
            .any(|column| self.transformations.contains_key(&column.name))
    }
}

impl fmt::Debug for MaskingTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaskingTableProvider")
            .field(
                "masked_fields",
                &self.transformations.keys().collect::<Vec<_>>(),
            )
            .finish()
    }
}

#[async_trait]
impl TableProvider for MaskingTableProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        let mut result = vec![TableProviderFilterPushDown::Unsupported; filters.len()];
        let mut safe = Vec::new();
        let mut safe_indices = Vec::new();
        for (index, filter) in filters.iter().enumerate() {
            if !self.references_masked(filter) {
                safe.push(*filter);
                safe_indices.push(index);
            }
        }
        for (index, pushdown) in safe_indices
            .into_iter()
            .zip(self.inner.supports_filters_pushdown(&safe)?)
        {
            result[index] = pushdown;
        }
        Ok(result)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let has_masked_filter = filters.iter().any(|filter| self.references_masked(filter));
        let safe_filters: Vec<Expr> = filters
            .iter()
            .filter(|filter| !self.references_masked(filter))
            .cloned()
            .collect();
        // A limit below a withheld masked-field predicate could discard matching rows before
        // MaskingExec transforms them and the residual FilterExec evaluates them.
        let safe_limit = if has_masked_filter { None } else { limit };
        let input = self
            .inner
            .scan(state, projection, &safe_filters, safe_limit)
            .await?;
        MaskingExec::try_new(input, Arc::clone(&self.transformations))
            .map(|plan| plan as Arc<dyn ExecutionPlan>)
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        // Inner statistics describe raw values. Publishing them for a masked provider could let
        // later optimizers reason about transformed fields using incompatible min/max values.
        None
    }
}

pub struct MaskingExec {
    input: Arc<dyn ExecutionPlan>,
    transformations: Transformations,
    properties: Arc<PlanProperties>,
}

impl MaskingExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        transformations: Transformations,
    ) -> Result<Arc<Self>> {
        let schema = input.schema();
        let projected: HashSet<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        for field in transformations
            .keys()
            .filter(|field| projected.contains(field.as_str()))
        {
            let data_type = schema.field_with_name(field)?.data_type();
            validate_type(field, data_type)?;
        }
        // Masking changes values and therefore invalidates raw-value ordering, equivalence, and
        // value-based partitioning claims. Preserve only the partition count and stream shape.
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(
                input.properties().output_partitioning().partition_count(),
            ),
            input.properties().emission_type,
            input.properties().boundedness,
        ));
        Ok(Arc::new(Self {
            input,
            transformations,
            properties,
        }))
    }
}

impl fmt::Debug for MaskingExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MaskingExec")
            .field("fields", &self.transformations.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl DisplayAs for MaskingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "MaskingExec: fields={:?}",
            self.transformations.keys().collect::<Vec<_>>()
        )
    }
}

impl ExecutionPlan for MaskingExec {
    fn name(&self) -> &str {
        "MaskingExec"
    }
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "MaskingExec expects one child, got {}",
                children.len()
            )));
        }
        Ok(Self::try_new(
            children.remove(0),
            Arc::clone(&self.transformations),
        )?)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let schema = self.schema();
        let stream_schema = Arc::clone(&schema);
        let transformations = Arc::clone(&self.transformations);
        let mapped = stream.and_then(move |batch| {
            let transformations = Arc::clone(&transformations);
            async move { transform_batch(batch, &transformations) }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            mapped,
        )))
    }
}

pub fn transform_batch(
    batch: RecordBatch,
    transformations: &HashMap<String, Transformation>,
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (index, field) in batch.schema().fields().iter().enumerate() {
        match transformations.get(field.name()) {
            Some(transformation) => columns.push(transform_array(
                batch.column(index),
                field.name(),
                transformation,
            )?),
            None => columns.push(Arc::clone(batch.column(index))),
        }
    }
    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn validate_type(field: &str, data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok(()),
        DataType::List(child) | DataType::LargeList(child) | DataType::FixedSizeList(child, _) => validate_type(field, child.data_type()),
        other => Err(DataFusionError::Execution(format!(
            "masked field [{field}] has unsupported Arrow type [{other:?}]; only strings and lists of strings are supported"
        ))),
    }
}

fn transform_array(
    array: &ArrayRef,
    field: &str,
    transformation: &Transformation,
) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Utf8 => {
            let values = array.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(Arc::new(StringArray::from_iter(values.iter().map(|value| value.map(|v| transform(v, transformation))))) as ArrayRef)
        }
        DataType::LargeUtf8 => {
            let values = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(Arc::new(LargeStringArray::from_iter(values.iter().map(|value| value.map(|v| transform(v, transformation))))) as ArrayRef)
        }
        DataType::Utf8View => {
            let values = array.as_any().downcast_ref::<StringViewArray>().unwrap();
            Ok(Arc::new(StringViewArray::from_iter(values.iter().map(|value| value.map(|v| transform(v, transformation))))) as ArrayRef)
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let data = array.to_data();
            let children = data.child_data();
            if children.len() != 1 {
                return Err(DataFusionError::Execution(format!("masked list field [{field}] has {} child arrays", children.len())));
            }
            let transformed = transform_array(&make_array(children[0].clone()), field, transformation)?;
            let rebuilt = data.into_builder().child_data(vec![transformed.to_data()]).build()?;
            Ok(make_array(rebuilt))
        }
        other => Err(DataFusionError::Execution(format!(
            "masked field [{field}] has unsupported Arrow type [{other:?}]; only strings and lists of strings are supported"
        ))),
    }
}

fn transform(value: &str, transformation: &Transformation) -> String {
    match transformation {
        Transformation::Blake2bPersonalized(personal) => {
            let mut params = Blake2bParams::new();
            params.hash_length(32).personal(personal);
            hex(params.hash(value.as_bytes()).as_bytes())
        }
        Transformation::Blake2bSalted(salt) => {
            let mut params = Blake2bParams::new();
            params.hash_length(32).salt(salt);
            hex(params.hash(value.as_bytes()).as_bytes())
        }
        Transformation::Sha256 => hex(&Sha256::digest(value.as_bytes())),
        Transformation::Sha512 => hex(&Sha512::digest(value.as_bytes())),
        Transformation::RegexReplace(replacements) => {
            replacements
                .iter()
                .fold(value.to_string(), |current, (pattern, replacement)| {
                    pattern
                        .replace_all(&current, replacement.as_str())
                        .into_owned()
                })
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::SessionContext;
    use parking_lot::Mutex;

    // Identical vectors are used in the test at
    // security/src/test/java/org/opensearch/security/privileges/dlsfls/FieldValueTransformationResolverTest.java.
    const GOLDEN_VECTORS: &str = r#"
        [
          {
            "name": "blake2b-256-personalized-ascii",
            "kind": "hash",
            "algorithm": "BLAKE2B_256_PERSONALIZED",
            "parameter_hex": "30313233343536373839616263646566",
            "input": "alice",
            "expected": "4ea22c69e3064517f269cfe53906f6492e99f19337b3ce659f1c49f5af3c5d34"
          },
          {
            "name": "blake2b-256-salted-unicode",
            "kind": "hash",
            "algorithm": "BLAKE2B_256_SALTED",
            "parameter_hex": "30313233343536373839616263646566",
            "input": "Grüße 東京",
            "expected": "8353eae0d97a4b105e7c2af5735d8bb84a85d07498fb4681fd139c56b84a2472"
          },
          {
            "name": "sha-256-ascii",
            "kind": "hash",
            "algorithm": "SHA_256",
            "parameter_hex": "",
            "input": "alice",
            "expected": "2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90"
          },
          {
            "name": "sha-512-unicode",
            "kind": "hash",
            "algorithm": "SHA_512",
            "parameter_hex": "",
            "input": "Grüße 東京",
            "expected": "04d2411562af0ca5edeab8e6a44d9ce53ffc44fed60242e7f797f62e4abab3f9e0f2adf6a36627f5021d6b3662b6be79f5452cbcc4d865685903e949589bdd71"
          },
          {
            "name": "regex-captures-and-sequential-replacement",
            "kind": "regex",
            "replacements": [
              {
                "pattern": "^([a-z]+)-([0-9]+)$",
                "replacement": "$2:$1"
              },
              {
                "pattern": "^7:",
                "replacement": "id="
              }
            ],
            "input": "alice-7",
            "expected": "id=alice"
          }
        ]
    "#;

    #[derive(Debug)]
    struct RecordingProvider {
        inner: Arc<dyn TableProvider>,
        limit: Mutex<Option<usize>>,
        filters: Mutex<Vec<Expr>>,
    }

    #[async_trait]
    impl TableProvider for RecordingProvider {
        fn schema(&self) -> SchemaRef {
            self.inner.schema()
        }

        fn table_type(&self) -> TableType {
            self.inner.table_type()
        }

        async fn scan(
            &self,
            state: &dyn Session,
            projection: Option<&Vec<usize>>,
            filters: &[Expr],
            limit: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            *self.limit.lock() = limit;
            *self.filters.lock() = filters.to_vec();
            self.inner.scan(state, projection, filters, limit).await
        }
    }

    #[test]
    fn shared_golden_vectors_have_stable_output() {
        let vectors = serde_json::from_str::<serde_json::Value>(GOLDEN_VECTORS)
            .unwrap()
            .as_array()
            .unwrap()
            .clone();
        for vector in vectors {
            let name = vector["name"].as_str().unwrap();
            let transformation = match vector["kind"].as_str().unwrap() {
                "hash" => match vector["algorithm"].as_str().unwrap() {
                    "BLAKE2B_256_PERSONALIZED" => Transformation::Blake2bPersonalized(decode_hex(
                        vector["parameter_hex"].as_str().unwrap(),
                    )),
                    "BLAKE2B_256_SALTED" => Transformation::Blake2bSalted(decode_hex(
                        vector["parameter_hex"].as_str().unwrap(),
                    )),
                    "SHA_256" => Transformation::Sha256,
                    "SHA_512" => Transformation::Sha512,
                    other => panic!("unknown golden-vector algorithm {other}"),
                },
                "regex" => Transformation::RegexReplace(
                    vector["replacements"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|replacement| {
                            (
                                Regex::new(replacement["pattern"].as_str().unwrap()).unwrap(),
                                replacement["replacement"].as_str().unwrap().to_string(),
                            )
                        })
                        .collect(),
                ),
                other => panic!("unknown golden-vector kind {other}"),
            };
            assert_eq!(
                transform(vector["input"].as_str().unwrap(), &transformation),
                vector["expected"].as_str().unwrap(),
                "golden vector {name}"
            );
        }
    }

    #[tokio::test]
    async fn masked_filter_and_limit_stay_above_masking() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "secret",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["bob", "alice"]))],
        )
        .unwrap();
        let inner = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        let recording = Arc::new(RecordingProvider {
            inner,
            limit: Mutex::new(None),
            filters: Mutex::new(Vec::new()),
        });
        let transformations = Arc::new(HashMap::from([(
            "secret".to_string(),
            Transformation::RegexReplace(vec![(Regex::new("^alice$").unwrap(), "MASK_A".into())]),
        )]));
        let ctx = SessionContext::new();
        ctx.register_table(
            "masked",
            MaskingTableProvider::wrap(recording.clone(), transformations),
        )
        .unwrap();

        let plan = ctx
            .sql("SELECT secret FROM masked WHERE secret = 'MASK_A' LIMIT 1")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_text = displayable(plan.as_ref()).indent(true).to_string();
        assert!(
            plan_text.find("FilterExec").unwrap() < plan_text.find("MaskingExec").unwrap(),
            "masked filter must execute above masking:\n{plan_text}"
        );
        let batches = collect(plan, ctx.task_ctx()).await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(*recording.limit.lock(), None);
        assert!(recording.filters.lock().is_empty());
    }

    #[test]
    fn masked_provider_hides_raw_statistics() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "secret",
            DataType::Utf8,
            false,
        )]));
        let inner: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap());
        let provider = MaskingTableProvider {
            inner,
            transformations: Arc::new(HashMap::from([(
                "secret".to_string(),
                Transformation::Sha256,
            )])),
        };

        assert!(provider.statistics().is_none());
    }

    #[test]
    fn masking_fails_closed_for_non_string_arrays() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "secret",
            DataType::Int32,
            true,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let error = transform_batch(
            batch,
            &HashMap::from([("secret".to_string(), Transformation::Sha256)]),
        )
        .unwrap_err()
        .to_string();

        assert!(
            error.contains("masked field [secret] has unsupported Arrow type [Int32]"),
            "{error}"
        );
    }

    #[test]
    fn decoder_rejects_duplicate_fields() {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&2_u32.to_be_bytes());
        for _ in 0..2 {
            encoded.extend_from_slice(&6_u32.to_be_bytes());
            encoded.extend_from_slice(b"secret");
            encoded.push(1); // hash
            encoded.push(2); // SHA-256
            encoded.extend_from_slice(&0_u32.to_be_bytes());
        }

        let error = decode(&encoded).unwrap_err().to_string();
        assert!(
            error.contains("duplicate field masking descriptor for [secret]"),
            "{error}"
        );
    }

    #[test]
    fn empty_descriptor_buffer_means_no_masking() {
        assert!(decode(&[]).unwrap().is_empty());
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        assert_eq!(value.len() % 2, 0);
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|chunk| u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap())
            .collect()
    }
}
