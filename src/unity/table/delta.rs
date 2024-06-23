use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::Constraints,
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use deltalake::DeltaTable;

pub struct UnityDeltaTable {
    table: DeltaTable,
}

impl UnityDeltaTable {
    pub async fn new(path: &str) -> Self {
        let table = deltalake::open_table(path).await.unwrap();
        UnityDeltaTable { table }
    }
}

#[async_trait]
impl TableProvider for UnityDeltaTable {
    fn as_any(&self) -> &dyn Any {
        TableProvider::as_any(&self.table)
    }

    fn schema(&self) -> SchemaRef {
        TableProvider::schema(&self.table)
    }

    fn constraints(&self) -> Option<&Constraints> {
        TableProvider::constraints(&self.table)
    }

    fn table_type(&self) -> TableType {
        // TODO: Something smart with view sharing?
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        TableProvider::scan(&self.table, state, projection, filters, limit).await
    }
}
