pub mod client;
pub mod table;
pub mod unity;

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

        let unity = Arc::new(unity::Unity::new().await);
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
