//! DataFusion analytical read engine (pure-Rust, no FFI).

use crate::DbkitError;
use crate::analytical::RecordBatch;
use crate::read::ReadEngine;
use crate::value::DbValue;
use ::datafusion::datasource::MemTable;
use ::datafusion::prelude::SessionContext;
use async_trait::async_trait;
use std::sync::Arc;

/// A DataFusion session used for analytical reads.
///
/// DataFusion is natively async and Arrow-based, so queries run directly on the
/// async runtime without a blocking pool.
pub struct DataFusionEngine {
    ctx: SessionContext,
}

impl DataFusionEngine {
    /// Create a fresh DataFusion session.
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Access the underlying session context (e.g. to register tables).
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }
}

impl Default for DataFusionEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReadEngine for DataFusionEngine {
    async fn query_arrow(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> Result<Vec<RecordBatch>, DbkitError> {
        // Decision: DataFusion does not support positional parameter binding
        // here yet. Reject params rather than risk an unsafe inline.
        if !params.is_empty() {
            return Err(DbkitError::DataFusion(
                "parameter binding is not supported by the DataFusion engine; \
                 inline values into the query instead"
                    .into(),
            ));
        }

        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| DbkitError::DataFusion(e.to_string()))?;

        df.collect()
            .await
            .map_err(|e| DbkitError::DataFusion(e.to_string()))
    }

    async fn load_table(
        &self,
        name: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), DbkitError> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = batches[0].schema();
        let table = MemTable::try_new(schema, vec![batches])
            .map_err(|e| DbkitError::DataFusion(e.to_string()))?;

        // Replace any existing table of the same name.
        let _ = self.ctx.deregister_table(name);
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(|e| DbkitError::DataFusion(e.to_string()))?;
        Ok(())
    }

    async fn drop_table(&self, name: &str) -> Result<(), DbkitError> {
        // Deregistering a name that was never registered returns Ok(None).
        self.ctx
            .deregister_table(name)
            .map_err(|e| DbkitError::DataFusion(e.to_string()))?;
        Ok(())
    }
}
