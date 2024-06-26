//! Unity Calalog implementation for DataFusion

use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::catalog::{CatalogProvider, CatalogProviderList};

use crate::{client::UnityClient, unity::catalog::Catalog};

use self::error::UnityError;

mod catalog;
pub mod error;
mod schema;
mod table;

/// Unity Catalog
pub struct Unity {
    client: Arc<UnityClient>,
    catalogs: HashMap<String, Arc<dyn CatalogProvider>>,
}

impl Unity {
    /// Initialize a new [`Unity`] instance with the given endpoint.
    ///
    /// # Example
    /// ```no_run
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # async {
    /// # use datafusion_unity_catalog::UnityError;
    /// use datafusion::catalog::CatalogProviderList;
    /// use datafusion_unity_catalog::Unity;
    ///
    /// let unity_catalog = Unity::try_new("http://localhost:8080/api/2.1/unity-catalog/").await?;
    /// assert_eq!(unity_catalog.catalog_names().len(), 1);
    /// # Ok::<(), UnityError>(()) };
    /// # Ok(()) }
    /// ```
    pub async fn try_new(endpoint: &str) -> Result<Self, UnityError> {
        let client = Arc::new(UnityClient::new(endpoint));
        let catalogs = HashMap::new();

        let mut unity = Self { client, catalogs };
        unity.fetch().await?;

        Ok(unity)
    }

    async fn _try_new_with_client(client: UnityClient) -> Result<Self, UnityError> {
        let client = Arc::new(client);
        let catalogs = HashMap::new();

        let mut unity = Self { client, catalogs };
        unity.fetch().await?;

        Ok(unity)
    }

    async fn fetch(&mut self) -> Result<(), UnityError> {
        let catalogs = self.client.list_catalogs().await?;
        for catalog in catalogs {
            let provider = Catalog::try_new(self.client.clone(), catalog.name()).await?;
            self.catalogs
                .insert(catalog.name().to_owned(), Arc::new(provider));
        }

        Ok(())
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
