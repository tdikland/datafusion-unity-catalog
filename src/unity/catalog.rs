use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::catalog::{schema::SchemaProvider, CatalogProvider};

use crate::{client::UnityClient, unity::UnitySchema};

pub struct Catalog {
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl Catalog {
    pub async fn try_new(
        client: Arc<UnityClient>,
        catalog_name: &str,
    ) -> Result<Catalog, UnityError> {
        let schemas = client.list_schemas(catalog_name).await;
        tracing::info!("Found schemas: {:?}", schemas);

        let mut schema_providers = HashMap::new();
        for schema in schemas.iter() {
            let provider: Arc<dyn SchemaProvider> =
                Arc::new(UnitySchema::try_new(client.clone(), catalog_name, &schema.name).await);
            schema_providers.insert(schema.name.clone(), provider);
        }

        Catalog {
            schemas: schema_providers,
        }
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
