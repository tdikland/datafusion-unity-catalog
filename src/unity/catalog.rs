use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::catalog::{schema::SchemaProvider, CatalogProvider};

use crate::{client::UnityClient, unity::schema::UnitySchema};

use super::error::UnityError;

pub struct Catalog {
    name: String,
    client: Arc<UnityClient>,
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl Catalog {
    pub async fn try_new(
        client: Arc<UnityClient>,
        catalog_name: &str,
    ) -> Result<Catalog, UnityError> {
        let mut catalog = Catalog {
            name: catalog_name.to_string(),
            client: client.clone(),
            schemas: HashMap::new(),
        };
        catalog.fetch().await?;
        Ok(catalog)
    }

    async fn fetch(&mut self) -> Result<(), UnityError> {
        let schemas = self.client.list_schemas(&self.name).await?;
        for schema in schemas {
            let provider =
                UnitySchema::try_new(self.client.clone(), &self.name, schema.name()).await?;
            self.schemas
                .insert(schema.name().to_owned(), Arc::new(provider));
        }

        Ok(())
    }
}

impl CatalogProvider for Catalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }

    // TODO: implement overwrite trait
    // fn register_schema(
    //     &self,
    //     name: &str,
    //     schema: Arc<dyn SchemaProvider>,
    // ) -> Result<Option<Arc<dyn SchemaProvider>>> {
    //     // use variables to avoid unused variable warnings
    //     let _ = name;
    //     let _ = schema;
    //     not_impl_err!("Registering new schemas is not supported")
    // }

    // TODO: implement overwrite trait
    // fn deregister_schema(
    //     &self,
    //     _name: &str,
    //     _cascade: bool,
    // ) -> Result<Option<Arc<dyn SchemaProvider>>> {
    //     not_impl_err!("Deregistering new schemas is not supported")
    // }
}
