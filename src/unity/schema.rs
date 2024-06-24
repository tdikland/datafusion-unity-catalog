use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::schema::SchemaProvider, datasource::TableProvider, error::DataFusionError,
};

use super::{error::UnityError, table::delta::UnityDeltaTable};
use crate::client::{Table, UnityClient};

pub struct UnitySchema {
    catalog_name: String,
    name: String,
    client: Arc<UnityClient>,
    tables: HashMap<String, Table>,
}

impl UnitySchema {
    pub async fn try_new(
        client: Arc<UnityClient>,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<UnitySchema, UnityError> {
        let mut schema = Self {
            catalog_name: catalog_name.to_owned(),
            name: schema_name.to_owned(),
            client: client.clone(),
            tables: HashMap::new(),
        };
        schema.fetch().await?;
        Ok(schema)
    }

    async fn fetch(&mut self) -> Result<(), UnityError> {
        let tables = self
            .client
            .list_tables(&self.catalog_name, &self.name)
            .await?;

        for table in tables {
            self.tables.insert(table.name().to_owned(), table);
        }

        Ok(())
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
            let table = Arc::new(UnityDeltaTable::new(&table.storage_location()).await);
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
