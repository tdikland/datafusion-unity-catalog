use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{schema::SchemaProvider, CatalogProvider, CatalogProviderList},
    datasource::TableProvider,
    error::DataFusionError,
};

use crate::{
    client::{DataSourceFormat, TableInfo, TableType as UcTableType, UnityClient},
    table::UnityDeltaTable,
};

mod catalog;
pub mod error;
mod schema;
pub mod unity;
pub mod table;

pub struct Unity {
    client: Arc<UnityClient>,
    catalogs: HashMap<String, Arc<dyn CatalogProvider>>,
}

impl Unity {
    pub async fn try_new() -> Self {
        let client = Arc::new(UnityClient::new());
        let unity = client.list_catalogs().await;
        tracing::info!("Found catalogs: {:?}", unity);

        let mut catalogs = HashMap::new();

        for catalog in unity.iter() {
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
        let schemas = client.list_schemas(catalog_name).await.unwrap();
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
    // TODO: check if schema can be derived from unity-catalog
    fn owner_name(&self) -> Option<&str> {
        None
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if name == "marksheet_uniform" {
            return Ok(None);
        }

        if self.tables.contains_key(name) {
            let table = self.tables.get(name).unwrap();
            let table =
                Arc::new(UnityDeltaTable::new(&table.storage_location.clone().unwrap()).await);
            Ok(Some(table))
        } else {
            Ok(None)
        }
    }

    // TODO: overwrite default implementation
    // fn register_table(
    //     &self,
    //     name: String,
    //     table: Arc<dyn TableProvider>,
    // ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
    //     todo!()
    // }

    // TODO: overwrite default implementation
    // fn deregister_table(
    //     &self,
    //     name: &str,
    // ) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
    //     todo!()
    // }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}
