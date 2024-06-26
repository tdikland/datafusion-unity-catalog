use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{Constraints, Statistics},
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown},
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
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        TableProvider::get_table_definition(&self.table)
    }

    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        TableProvider::get_logical_plan(&self.table)
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        TableProvider::get_column_default(&self.table, column)
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

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, DataFusionError> {
        TableProvider::supports_filters_pushdown(&self.table, filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        TableProvider::statistics(&self.table)
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        TableProvider::insert_into(&self.table, state, input, overwrite).await
    }
}
