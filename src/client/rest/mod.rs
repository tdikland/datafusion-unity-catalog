use std::fmt;

use reqwest::Client;
use url::Url;

use self::types::{ListCatalogsResponse, ListSchemasResponse, ListTablesResponse};

pub mod types;

pub struct UnityRestClient {
    base: Url,
    client: Client,
}

impl UnityRestClient {
    pub fn new(base_url: &str) -> UnityRestClient {
        let client = Client::new();
        Self {
            base: Url::parse(base_url).expect("valid"),
            client,
        }
    }

    pub async fn list_catalogs(
        &self,
        page_token: Option<&str>,
        max_results: Option<&str>,
    ) -> Result<ListCatalogsResponse, RestClientError> {
        let mut url = self.base.clone();
        add_path_segment(&mut url, "catalogs");
        add_query_param(&mut url, "page_token", page_token);
        add_query_param(&mut url, "max_results", max_results);

        self.client
            .get(url)
            .send()
            .await?
            .json()
            .await
            .map_err(From::from)
    }

    pub async fn list_schemas(
        &self,
        catalog_name: &str,
        page_token: Option<&str>,
        max_results: Option<&str>,
    ) -> Result<ListSchemasResponse, RestClientError> {
        let mut url = self.base.clone();
        add_path_segment(&mut url, "schemas");
        add_query_param(&mut url, "catalog_name", Some(catalog_name));
        add_query_param(&mut url, "page_token", page_token);
        add_query_param(&mut url, "max_results", max_results);

        self.client
            .get(url)
            .send()
            .await?
            .json::<ListSchemasResponse>()
            .await
            .map_err(From::from)
    }

    pub async fn list_tables(
        &self,
        catalog_name: &str,
        schema_name: &str,
        page_token: Option<&str>,
        max_results: Option<&str>,
    ) -> Result<ListTablesResponse, RestClientError> {
        let mut url = self.base.clone();
        add_path_segment(&mut url, "tables");
        add_query_param(&mut url, "catalog_name", Some(catalog_name));
        add_query_param(&mut url, "schema_name", Some(schema_name));
        add_query_param(&mut url, "page_token", page_token);
        add_query_param(&mut url, "max_results", max_results);

        self.client
            .get(url)
            .send()
            .await?
            .json::<ListTablesResponse>()
            .await
            .map_err(From::from)
    }
}

fn add_path_segment(url: &mut Url, segment: &str) {
    let mut path = url.path_segments_mut().expect("valid base");
    path.push(segment);
}

fn add_query_param<T: AsRef<str>>(url: &mut Url, key: &str, value: Option<T>) {
    if let Some(value) = value {
        url.query_pairs_mut().append_pair(key, value.as_ref());
    }
}

#[derive(Debug)]
pub enum RestClientError {
    Unknown(String),
}

impl fmt::Display for RestClientError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Unknown(e) => write!(f, "Unknown error: {}", e),
        }
    }
}

impl std::error::Error for RestClientError {}

impl From<reqwest::Error> for RestClientError {
    fn from(e: reqwest::Error) -> Self {
        tracing::error!("REQWEST ERROR: {:?}", e);
        Self::Unknown(e.to_string())
    }
}
