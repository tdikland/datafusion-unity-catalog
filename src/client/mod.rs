use self::{error::ClientError, rest::UnityRestClient};

pub mod error;
pub mod rest;

pub struct UnityClient {
    rest_client: UnityRestClient,
}

impl UnityClient {
    pub fn new(endpoint: &str) -> UnityClient {
        let rest_client = UnityRestClient::new(endpoint);
        Self { rest_client }
    }

    pub async fn list_catalogs(&self) -> Result<Vec<Catalog>, ClientError> {
        let mut catalogs = Vec::new();
        let mut page_token = None;
        loop {
            let response = self
                .rest_client
                .list_catalogs(page_token.as_deref(), None)
                .await
                .unwrap();
            catalogs.extend(response.catalogs.into_iter().map(|c| c.name));
            page_token = response.next_page_token;
            if page_token.is_none() || page_token.as_ref().is_some_and(|s| s.is_empty()) {
                let c = catalogs
                    .into_iter()
                    .map(|name| Catalog::new(name))
                    .collect();
                break Ok(c);
            }
        }
    }

    pub async fn list_schemas(&self, catalog_name: &str) -> Result<Vec<Schema>, ClientError> {
        let mut schemas = Vec::new();
        let mut page_token = None;
        loop {
            let response = self
                .rest_client
                .list_schemas(catalog_name, page_token.as_deref(), None)
                .await
                .unwrap();
            schemas.extend(response.schemas.into_iter().map(|s| s.name));
            page_token = response.next_page_token;
            if page_token.is_none() || page_token.as_ref().is_some_and(|s| s.is_empty()) {
                let s = schemas
                    .into_iter()
                    .map(|name| Schema::new(catalog_name.to_string(), name))
                    .collect();
                break Ok(s);
            }
        }
    }

    pub async fn list_tables(
        &self,
        catalog_name: &str,
        schema_name: &str,
    ) -> Result<Vec<Table>, ClientError> {
        let mut tables = Vec::new();
        let mut page_token = None;
        loop {
            let response = self
                .rest_client
                .list_tables(catalog_name, schema_name, page_token.as_deref(), None)
                .await
                .unwrap();
            tables.extend(response.tables);
            page_token = response.next_page_token;
            if page_token.is_none() || page_token.as_ref().is_some_and(|s| s.is_empty()) {
                let t = tables
                    .into_iter()
                    .map(|table_info| {
                        Table::new(
                            catalog_name.to_string(),
                            schema_name.to_string(),
                            table_info.name.expect("table name"),
                            table_info.storage_location.expect("storage location"),
                        )
                    })
                    .collect();
                break Ok(t);
            }
        }
    }
}

pub struct Catalog {
    name: String,
}

impl Catalog {
    pub fn new(name: String) -> Catalog {
        Catalog { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub struct Schema {
    catalog_name: String,
    name: String,
}

impl Schema {
    pub fn new(catalog_name: String, name: String) -> Schema {
        Schema { catalog_name, name }
    }

    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

pub struct Table {
    catalog_name: String,
    schema_name: String,
    name: String,
    storage_location: String,
}

impl Table {
    pub fn new(
        catalog_name: String,
        schema_name: String,
        name: String,
        storage_location: String,
    ) -> Table {
        Table {
            catalog_name,
            schema_name,
            name,
            storage_location,
        }
    }

    pub fn catalog_name(&self) -> &str {
        &self.catalog_name
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn storage_location(&self) -> &str {
        &self.storage_location
    }
}
