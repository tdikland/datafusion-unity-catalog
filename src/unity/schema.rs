use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::schema::SchemaProvider, datasource::TableProvider, error::DataFusionError,
};

use crate::{
    client::{TableInfo, UnityClient},
    table::UnityDeltaTable,
};

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
