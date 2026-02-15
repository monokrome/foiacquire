//! PostgreSQL TLS connection helpers using rustls.
//!
//! Provides TLS connectors for both raw tokio-postgres connections
//! and diesel-async connection pools. TLS is required by default;
//! use `--no-tls` or `FOIA_NO_TLS=1` to disable.

use diesel::ConnectionError;
use diesel_async::AsyncPgConnection;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use rustls::ClientConfig;
use tokio_postgres_rustls::MakeRustlsConnect;
use tracing::warn;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn build_rustls_config() -> Result<ClientConfig, BoxError> {
    let result = rustls_native_certs::load_native_certs();

    if !result.errors.is_empty() {
        for e in &result.errors {
            warn!("Error loading system certificates: {}", e);
        }
    }

    let mut root_store = rustls::RootCertStore::empty();
    let mut loaded = 0u32;

    for cert in result.certs {
        match root_store.add(cert) {
            Ok(()) => loaded += 1,
            Err(e) => warn!("Skipping invalid system certificate: {}", e),
        }
    }

    if loaded == 0 {
        return Err("no valid system certificates found".into());
    }

    Ok(ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}

fn make_tls_connector() -> Result<MakeRustlsConnect, BoxError> {
    Ok(MakeRustlsConnect::new(build_rustls_config()?))
}

pub fn establish_tls_connection(
    url: &str,
) -> BoxFuture<'_, diesel::ConnectionResult<AsyncPgConnection>> {
    let fut = async {
        let tls = make_tls_connector()
            .map_err(|e| ConnectionError::BadConnection(format!("TLS setup failed: {}", e)))?;
        let (client, conn) = tokio_postgres::connect(url, tls)
            .await
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        AsyncPgConnection::try_from_client_and_connection(client, conn).await
    };
    fut.boxed()
}

/// Connect to PostgreSQL and spawn the connection task.
///
/// The connection future is spawned as a background tokio task. Connection
/// errors after initial setup are logged via tracing.
pub async fn connect_raw(url: &str, no_tls: bool) -> Result<tokio_postgres::Client, BoxError> {
    if no_tls {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });
        Ok(client)
    } else {
        let tls = make_tls_connector()?;
        let (client, connection) = tokio_postgres::connect(url, tls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });
        Ok(client)
    }
}
