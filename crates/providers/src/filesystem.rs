use std::any::Any;
use std::path;
use std::sync::{Arc, LazyLock};
use std::time::SystemTime;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef, TimeUnit};
use datafusion::catalog::TableFunctionImpl;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{DisplayAs, PlanProperties};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;

/// A table function that returns a table provider with the value as a single column
#[derive(Default, Debug)]
pub struct FilesystemListingFunction {}

impl TableFunctionImpl for FilesystemListingFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        todo!("Implement call for FilesystemListingFunction");
        //    let Some(Expr::Literal(ScalarValue::Int64(Some(value)))) = exprs.get(0) else {
        //        return datafusion::common::plan_err!("First argument must be an integer");
        //    };
        //
        //    // Create the schema for the table
        //    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        //
        //// Create a single RecordBatch with the value as a single column
        //let batch = RecordBatch::try_new(
        //        schema.clone(),
        //        vec![Arc::new(Int64Array::from(vec![*value]))],
        //    )?;
        //
        //    // Create a MemTable plan that returns the RecordBatch
        //    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
        //
        //    Ok(Arc::new(provider))
    }
}

static LISTING_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let mut builder = SchemaBuilder::new();

    // From std::fs::DirEntry
    builder.push(Field::new("path", DataType::Utf8, true));

    // From std::fs::Metadata
    builder.push(Field::new("is_dir", DataType::Boolean, true));
    builder.push(Field::new("is_file", DataType::Boolean, true));
    builder.push(Field::new("is_symlink", DataType::Boolean, true));
    builder.push(Field::new("size", DataType::UInt64, true));
    builder.push(Field::new(
        "created",
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    ));
    builder.push(Field::new(
        "modified",
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    ));
    builder.push(Field::new(
        "accessed",
        DataType::Timestamp(TimeUnit::Second, None),
        true,
    ));
    builder.push(Field::new("contents", DataType::LargeBinary, true));

    Arc::new(builder.finish())
});

/// A TableProvider that provides access to a filesystem at a given root
#[derive(Debug)]
pub struct FilesystemTableProvider {
    root: path::PathBuf,
    schema: SchemaRef,
}

impl FilesystemTableProvider {
    pub fn new(root: impl Into<path::PathBuf>) -> Self {
        Self {
            root: root.into(),
            schema: LISTING_SCHEMA.clone(),
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for FilesystemTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        println!("Scan projection = {:?}", projection);
        let schema = if let Some(indices) = projection {
            if indices.is_empty() {
                self.schema.clone()
            } else {
                Arc::new(self.schema.project(indices)?)
            }
        } else {
            self.schema.clone()
        };

        return Ok(Arc::new(FilesystemExec::new(&self.root, schema, limit)));
    }

    #[doc = "Get the type of this table for metadata/catalog purposes."]
    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }
}

#[derive(Clone, Debug)]
struct FilesystemExec {
    root: path::PathBuf,
    schema: SchemaRef,
    limit: Option<usize>,
    prop_cache: datafusion::physical_plan::PlanProperties,
}

impl FilesystemExec {
    pub fn new(root: impl Into<path::PathBuf>, schema: SchemaRef, limit: Option<usize>) -> Self {
        FilesystemExec {
            root: root.into(),
            schema: schema.clone(),
            limit,
            prop_cache: Self::compute_properties(schema, 1),
        }
    }

    fn compute_properties(schema: SchemaRef, n_partitions: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(n_partitions),
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl DisplayAs for FilesystemExec {
    fn fmt_as(
        &self,
        format_type: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        use datafusion::physical_plan::DisplayFormatType;
        match format_type {
            DisplayFormatType::Default => {
                write!(f, "{}", self.name())
            }
            DisplayFormatType::Verbose => {
                write!(f, "{}: root={}", self.name(), self.root.display())
            }
        }
    }
}

impl ExecutionPlan for FilesystemExec {
    fn name(&self) -> &'static str {
        "FilesystemExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.prop_cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(format!(
                "FilesystemExec does not support children, attempted to add {}",
                children.len()
            )))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream, DataFusionError> {
        use futures::StreamExt as _;

        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                concat!(
                    "Partition '{}' requested, but only one partition ",
                    "supported for filesystem listings"
                ),
                partition
            )));
        }

        let root = self.root.clone();
        let schema_ref = self.schema.clone();
        let s = async_stream::stream! {
            let listing: Vec<tokio::fs::DirEntry> = tokio_stream::wrappers::ReadDirStream::new(
                tokio::fs::read_dir(&root).await?)
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<std::io::Result<_>>()
                    .map_err(|error| {
                        DataFusionError::Internal(
                            format!(
                                "Error encountered while listing directory '{}': {}",
                                root.display(),
                                error))
                    })?;
            let metadatas = {
                let mut tmp = Vec::with_capacity(listing.len());
                for entry in &listing {
                    tmp.push(entry.metadata().await.map(Some).unwrap_or(None))
                }
                tmp
            };

            let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![];
            for f in &schema_ref.fields {
                match f.name().as_str() {
                    "path" => {
                        columns.push(Arc::new(arrow::array::StringArray::from_iter(listing.iter().map(|entry| {
                            Some(entry.path().display().to_string())
                        }))))
                    },
                    "is_dir" => {
                        columns.push(Arc::new(arrow::array::BooleanArray::from_iter(metadatas.iter().map(|meta| {
                            meta.as_ref().map(|m| m.is_dir())
                        }))))
                    },
                    "is_file" => {
                        columns.push(Arc::new(arrow::array::BooleanArray::from_iter(metadatas.iter().map(|meta| {
                            meta.as_ref().map(|m| m.is_file())
                        }))))
                    },
                    "is_symlink" => {
                        columns.push(Arc::new(arrow::array::BooleanArray::from_iter(metadatas.iter().map(|meta| {
                            meta.as_ref().map(|m| m.is_file())
                        }))))
                    },
                    "size" => {
                        columns.push(Arc::new(arrow::array::UInt64Array::from_iter(metadatas.iter().map(|meta| {
                            meta.as_ref().map(|m| m.len())
                        }))))
                    },
                    "created" => {
                        columns.push(Arc::new(arrow::array::TimestampSecondArray::from_iter(
                            metadatas.iter().map(|meta| {
                                meta.as_ref().map(|m| {
                                    let Ok(time) = m.created() else {
                                        return 0
                                    };
                                    let Ok(dur) = time.duration_since(SystemTime::UNIX_EPOCH) else {
                                        return 0
                                    };

                                    dur.as_secs() as i64
                                })
                            }))))
                    },
                    "modified" => {
                        columns.push(Arc::new(arrow::array::TimestampSecondArray::from_iter(
                            metadatas.iter().map(|meta| {
                                meta.as_ref().map(|m| {
                                    let Ok(time) = m.modified() else {
                                        return 0
                                    };
                                    let Ok(dur) = time.duration_since(SystemTime::UNIX_EPOCH) else {
                                        return 0
                                    };

                                    dur.as_secs() as i64
                                })
                            }))))
                    },
                    "accessed" => {
                        columns.push(Arc::new(arrow::array::TimestampSecondArray::from_iter(
                            metadatas.iter().map(|meta| {
                                meta.as_ref().map(|m| {
                                    let Ok(time) = m.accessed() else {
                                        return 0
                                    };
                                    let Ok(dur) = time.duration_since(SystemTime::UNIX_EPOCH) else {
                                        return 0
                                    };

                                    dur.as_secs() as i64
                                })
                            }))))
                    },
                    "contents" => {
                        columns.push(Arc::new(arrow::array::LargeBinaryArray::from_iter(
                            (0..listing.len()).map(|_| Option::<&[u8]>::None)
                        )))
                    },
                    name => Err(DataFusionError::Internal(format!("Unrecognized field {name}")))?
                }
            }

            let batch = RecordBatch::try_new(schema_ref, columns)?;
            yield Ok(batch)
        };
        Ok(Box::pin(
            datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                self.schema.clone(),
                s,
            ),
        ))
    }
}
