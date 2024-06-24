mod client;
// mod table;
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

        let unity = Arc::new(unity::Unity::try_new("http://127.0.0.1:8080/api/2.1/unity-catalog/").await.unwrap());
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
