//! PostgreSQL TLS connection helpers using rustls.
//!
//! Provides TLS connectors for both raw tokio-postgres connections
//! and diesel-async connection pools. TLS is required by default;
//! use `--no-tls` or `FOIACQUIRE_NO_TLS=1` to disable.

use diesel::ConnectionError;
use diesel_async::AsyncPgConnection;
use futures_util::future::BoxFuture;
use futures_util::FutureExt;
use rustls::ClientConfig;
use tokio_postgres_rustls::MakeRustlsConnect;

fn build_rustls_config() -> ClientConfig {
    let mut root_store = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().expect("failed to load native certificates")
    {
        root_store.add(cert).ok();
    }

    ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth()
}

pub fn make_tls_connector() -> MakeRustlsConnect {
    MakeRustlsConnect::new(build_rustls_config())
}

pub fn establish_tls_connection(
    url: &str,
) -> BoxFuture<'_, diesel::ConnectionResult<AsyncPgConnection>> {
    let fut = async {
        let tls = make_tls_connector();
        let (client, conn) = tokio_postgres::connect(url, tls)
            .await
            .map_err(|e| ConnectionError::BadConnection(e.to_string()))?;

        AsyncPgConnection::try_from_client_and_connection(client, conn).await
    };
    fut.boxed()
}

/// Connect to PostgreSQL and spawn the connection task.
///
/// Returns just the `Client`. The connection future is spawned as a
/// background tokio task automatically.
pub async fn connect_raw(
    url: &str,
    no_tls: bool,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    if no_tls {
        let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });
        Ok(client)
    } else {
        let tls = make_tls_connector();
        let (client, connection) = tokio_postgres::connect(url, tls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!("PostgreSQL connection error: {}", e);
            }
        });
        Ok(client)
    }
}
