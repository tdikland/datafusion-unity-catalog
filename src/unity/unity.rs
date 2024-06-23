use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{schema::SchemaProvider, CatalogProvider, CatalogProviderList},
    datasource::TableProvider,
    error::DataFusionError,
};

use crate::{
    unity::UnityCatalog,
    client::{DataSourceFormat, TableInfo, TableType as UcTableType, UnityClient},
    table::UnityDeltaTable,
};

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

    // TODO: implement this with remote catalog?
    // function signature is very awkward.
    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
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
