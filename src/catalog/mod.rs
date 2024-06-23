use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::{schema::SchemaProvider, CatalogProvider, CatalogProviderList},
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};

use crate::client::{DataSourceFormat, TableInfo, TableType as UcTableType, UnityClient};

pub struct Unity {
    client: Arc<UnityClient>,
    catalogs: HashMap<String, Arc<dyn CatalogProvider>>,
}

impl Unity {
    pub async fn new() -> Unity {
        let client = Arc::new(UnityClient::new());
        let unity = client.list_catalogs().await;
        tracing::info!("Found catalogs: {:?}", unity);

        let mut catalogs = HashMap::new();

        for catalog in unity.iter() {
            if catalog.name == "test1" {
                continue;
            }
            let provider: Arc<dyn CatalogProvider> =
                Arc::new(UnityCatalog::try_new(client.clone(), &catalog.name).await);
            catalogs.insert(catalog.name.clone(), provider);
        }

        Unity { client, catalogs }
    }
}

impl CatalogProviderList for Unity {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        todo!()
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.keys().cloned().collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.get(name).cloned()
    }
}

pub struct UnityCatalog {
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl UnityCatalog {
    pub async fn try_new(client: Arc<UnityClient>, catalog_name: &str) -> UnityCatalog {
        let schemas = client.list_schemas(catalog_name).await;
        tracing::info!("Found schemas: {:?}", schemas);

        let mut schema_providers = HashMap::new();
        for schema in schemas.iter() {
            let provider: Arc<dyn SchemaProvider> =
                Arc::new(UnitySchema::try_new(client.clone(), catalog_name, &schema.name).await);
            schema_providers.insert(schema.name.clone(), provider);
        }

        UnityCatalog {
            schemas: schema_providers,
        }
    }
}

impl CatalogProvider for UnityCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let s = self.schemas.keys().cloned().collect();
        tracing::warn!("FETCHING schema_names: {:?}", s);
        s
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
        // let mut tables = HashMap::new();
        // tables.insert(
        //     "table1".to_string(),
        //     TableInfo {
        //         table_id: Some(String::from("1")),
        //         name: Some(String::from("table1")),
        //         catalog_name: Some(String::from("table1")),
        //         schema_name: Some(String::from("table1")),
        //         table_type: Some(UcTableType::Managed),
        //         data_source_format: Some(DataSourceFormat::Parquet),
        //         columns: vec![],
        //         storage_location: Some(String::from("table1")),
        //         comment: Some(String::from("table1")),
        //         properties: HashMap::new(),
        //         created_at: None,
        //         updated_at: None,
        //     },
        // );

        // Some(Arc::new(UnitySchema {
        //     client: Arc::new(UnityClient::new()),
        //     tables: tables,
        // }))
    }
}

pub struct UnitySchema {
    client: Arc<UnityClient>,
    tables: HashMap<String, TableInfo>,
}

impl UnitySchema {
    pub async fn try_new(
        client: Arc<UnityClient>,
        catalog_name: &str,
        schema_name: &str,
    ) -> UnitySchema {
        let tables: HashMap<String, TableInfo> = client
            .list_tables(catalog_name, schema_name)
            .await
            .into_iter()
            .map(|table| (table.name.clone().unwrap(), table))
            .collect();

        tracing::info!("Found tables: {:?}", tables);

        for t in tables.iter() {
            dbg!(&t);
        }

        UnitySchema { client, tables }
    }
}

#[async_trait]
impl SchemaProvider for UnitySchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(Some(Arc::new(Tab {})))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

struct Tab {}

#[async_trait]
impl TableProvider for Tab {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    /// Get a reference to the schema for this table
    fn schema(&self) -> SchemaRef {
        todo!()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        todo!()
    }
}
