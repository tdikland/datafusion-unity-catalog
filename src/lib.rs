//! DataFusion catalog implementation for Unity Catalog
//!
//! # Example
//!
//! To use Unity Catalog with DataFusion, you need a running Unity Catalog
//! server. An example of how to start a Unity Catalog server can be found in the
//! [Unity Catalog repository](https://github.com/unitycatalog/unitycatalog).
//! When running the reference server, you can use the following code to connect
//!
//! ```no_run
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # async {
//! # use std::sync::Arc;
//! use datafusion::prelude::*;
//! use datafusion_unity_catalog::{Unity, UnityError};
//!
//! let mut ctx = SessionContext::new();
//! let unity_endpoint = "http://127.0.0.1:8080/api/2.1/unity-catalog/";
//! let unity_catalog = Unity::try_new(unity_endpoint).await?;
//! ctx.register_catalog_list(Arc::new(unity_catalog));
//!
//! ctx.sql("SELECT * FROM unity.default.marksheet;")
//!     .await?
//!     .show()
//!     .await?;
//! # Ok::<(), Box<dyn std::error::Error + Send + Sync + 'static>>(()) };
//! # Ok(()) }
//! ```

#![warn(missing_docs)]

mod client;
mod unity;

pub use unity::{error::UnityError, Unity};

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn test_list_catalogs() {
        tracing_subscriber::fmt::init();

        let cfg = SessionConfig::new().with_information_schema(true);
        let mut ctx = SessionContext::new_with_config(cfg);

        let unity = Arc::new(
            unity::Unity::try_new("http://127.0.0.1:8080/api/2.1/unity-catalog/")
                .await
                .unwrap(),
        );
        ctx.register_catalog_list(unity);

        ctx.sql("SELECT * FROM information_schema.tables;")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();

        ctx.sql("SELECT * FROM unity.default.numbers;")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();

        ctx.sql("SELECT * FROM unity.default.marksheet;")
            .await
            .unwrap()
            .show()
            .await
            .unwrap();

        assert!(false)
    }
}
