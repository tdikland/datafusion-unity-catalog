use std::collections::HashMap;

use reqwest::Client;
use serde::Deserialize;

pub struct UnityClient {
    client: Client,
}

impl UnityClient {
    pub fn new() -> UnityClient {
        let client = Client::new();
        Self { client }
    }

    pub async fn list_catalogs(&self) -> Vec<CatalogInfo> {
        self.client
            .get("http://127.0.0.1:8080/api/2.1/unity-catalog/catalogs")
            .send()
            .await
            .unwrap()
            .json::<ListCatalogsResponse>()
            .await
            .unwrap()
            .catalogs
    }

    pub async fn list_schemas(&self, catalog_name: &str) -> Vec<SchemaInfo> {
        let url = format!(
            "http://127.0.0.1:8080/api/2.1/unity-catalog/schemas?catalog_name={}",
            catalog_name
        );
        self.client
            .get(&url)
            .send()
            .await
            .unwrap()
            .json::<ListSchemasResponse>()
            .await
            .unwrap()
            .schemas
    }

    pub async fn list_tables(&self, catalog_name: &str, schema_name: &str) -> Vec<TableInfo> {
        let url = format!(
            "http://127.0.0.1:8080/api/2.1/unity-catalog/tables?catalog_name={}&schema_name={}",
            catalog_name, schema_name
        );
        self.client
            .get(&url)
            .send()
            .await
            .unwrap()
            .json::<ListTablesResponse>()
            .await
            .unwrap()
            .tables
    }
}

#[derive(Debug, Deserialize)]
pub struct ListCatalogsResponse {
    pub catalogs: Vec<CatalogInfo>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct CatalogInfo {
    pub id: String,
    pub name: String,
    pub comment: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ListSchemasResponse {
    pub schemas: Vec<SchemaInfo>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SchemaInfo {
    pub schema_id: String,
    pub name: String,
    pub catalog_name: Option<String>,
    pub comment: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
    pub full_name: Option<String>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct ListTablesResponse {
    pub tables: Vec<TableInfo>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TableInfo {
    pub table_id: Option<String>,
    pub name: Option<String>,
    pub catalog_name: Option<String>,
    pub schema_name: Option<String>,
    pub table_type: Option<TableType>,
    pub data_source_format: Option<DataSourceFormat>,
    pub columns: Vec<ColumnInfo>,
    pub storage_location: Option<String>,
    pub comment: Option<String>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TableType {
    Managed,
    External,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum DataSourceFormat {
    Delta,
    Csv,
    Json,
    Avro,
    Parquet,
    Orc,
    Text,
}

#[derive(Debug, Deserialize)]
pub struct ColumnInfo {
    name: Option<String>,
    type_text: Option<String>,
    type_json: Option<String>,
    type_name: Option<ColumnTypeName>,
    type_precision: Option<i32>,
    type_scale: Option<i32>,
    type_interval_type: Option<String>,
    position: Option<i32>,
    comment: Option<String>,
    nullable: bool,
    partition_index: Option<i32>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ColumnTypeName {
    Boolean,
    Byte,
    Short,
    Int,
    Long,
    Float,
    Double,
    Date,
    Timestamp,
    TimestampNtz,
    String,
    Binary,
    Decimal,
    Interval,
    Array,
    Struct,
    Map,
    Char,
    Null,
    UserDefinedType,
    TableType,
}
