use serde::Deserialize;
use std::collections::HashMap;

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
    #[serde(default)]
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
    pub name: Option<String>,
    pub type_text: Option<String>,
    pub type_json: Option<String>,
    pub type_name: Option<ColumnTypeName>,
    pub type_precision: Option<i32>,
    pub type_scale: Option<i32>,
    pub type_interval_type: Option<String>,
    pub position: Option<i32>,
    pub comment: Option<String>,
    pub nullable: bool,
    pub partition_index: Option<i32>,
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
