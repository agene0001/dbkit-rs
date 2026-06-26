//! ClickHouse remote source over the HTTP interface.
//!
//! Queries are sent with `FORMAT ArrowStream`, so ClickHouse returns an Arrow
//! IPC stream that is parsed straight into `RecordBatch`es — no row-by-row
//! decoding.

use crate::DbkitError;
use crate::analytical::RecordBatch;
use crate::analytical::arrow::ipc::reader::StreamReader;
use crate::analytical::arrow::ipc::writer::StreamWriter;
use crate::remote::{RemoteSink, RemoteSource};
use crate::value::DbValue;
use async_trait::async_trait;
use std::io::Cursor;

/// A handle to a ClickHouse server queried over HTTP.
///
/// ```ignore
/// let ch = ClickHouseSource::new("http://localhost:8123")
///     .database("analytics")
///     .credentials("default", "");
/// let batches = ch.query_arrow("SELECT count() FROM events", &[]).await?;
/// ```
pub struct ClickHouseSource {
    client: reqwest::Client,
    url: String,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
}

impl ClickHouseSource {
    /// Point at a ClickHouse HTTP endpoint, e.g. `http://localhost:8123`.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.into(),
            database: None,
            user: None,
            password: None,
        }
    }

    /// Set the default database for queries.
    pub fn database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }

    /// Set HTTP basic-auth credentials.
    pub fn credentials(mut self, user: impl Into<String>, password: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self.password = Some(password.into());
        self
    }
}

#[async_trait]
impl RemoteSource for ClickHouseSource {
    async fn query_arrow(
        &self,
        sql: &str,
        params: &[DbValue],
    ) -> Result<Vec<RecordBatch>, DbkitError> {
        if !params.is_empty() {
            return Err(DbkitError::Remote(
                "the ClickHouse source does not support bind parameters yet; \
                 inline values into the query"
                    .into(),
            ));
        }

        // ArrowStream yields an Arrow IPC stream we can decode directly.
        let body = format!("{sql}\nFORMAT ArrowStream");

        let mut req = self.client.post(&self.url).body(body);
        if let Some(db) = &self.database {
            req = req.query(&[("database", db)]);
        }
        if let Some(user) = &self.user {
            req = req.basic_auth(user, self.password.as_deref());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| DbkitError::Remote(e.to_string()))?;

        // ClickHouse reports errors in the response body, so surface it.
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(DbkitError::Remote(format!(
                "ClickHouse returned {status}: {}",
                text.trim()
            )));
        }

        let bytes = resp
            .bytes()
            .await
            .map_err(|e| DbkitError::Remote(e.to_string()))?;

        let reader = StreamReader::try_new(Cursor::new(bytes), None)
            .map_err(|e| DbkitError::Remote(format!("arrow decode: {e}")))?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| DbkitError::Remote(format!("arrow decode: {e}")))
    }
}

#[async_trait]
impl RemoteSink for ClickHouseSource {
    async fn write_arrow(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
    ) -> Result<(), DbkitError> {
        if batches.is_empty() {
            return Ok(());
        }

        // Encode the batches as an Arrow IPC stream for `FORMAT ArrowStream`.
        let schema = batches[0].schema();
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema)
                .map_err(|e| DbkitError::Remote(format!("arrow encode: {e}")))?;
            for batch in &batches {
                writer
                    .write(batch)
                    .map_err(|e| DbkitError::Remote(format!("arrow encode: {e}")))?;
            }
            writer
                .finish()
                .map_err(|e| DbkitError::Remote(format!("arrow encode: {e}")))?;
        }

        let mut req = self
            .client
            .post(&self.url)
            .query(&[("query", format!("INSERT INTO {table} FORMAT ArrowStream"))])
            .body(buf);
        if let Some(db) = &self.database {
            req = req.query(&[("database", db)]);
        }
        if let Some(user) = &self.user {
            req = req.basic_auth(user, self.password.as_deref());
        }

        let resp = req
            .send()
            .await
            .map_err(|e| DbkitError::Remote(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(DbkitError::Remote(format!(
                "ClickHouse returned {status}: {}",
                text.trim()
            )));
        }
        Ok(())
    }
}
