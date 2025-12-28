//! Repository utilities.

use diesel::result::DatabaseErrorInformation;
#[cfg(feature = "postgres")]
use std::error::Error;

/// Execute a database operation on a diesel_context::DbPool.
///
/// This macro eliminates the repetitive `match &self.pool { ... }` pattern found
/// throughout the diesel_* repository code. It handles connection acquisition and
/// error conversion for both database backends.
///
/// Note: This is for the legacy diesel_context::DbPool. For the newer pool::DbPool,
/// use the `with_conn!` macro from pool.rs.
///
/// # Usage
///
/// ```ignore
/// with_diesel_conn!(self.pool, conn, {
///     diesel::select(count_star())
///         .first::<i64>(&mut conn)
///         .await
/// })
/// ```
#[macro_export]
macro_rules! with_diesel_conn {
    ($pool:expr, $conn:ident, $body:expr) => {{
        match &$pool {
            $crate::repository::diesel_context::DbPool::Sqlite(pool) => {
                let mut $conn = pool.get().await?;
                $body
            }
            #[cfg(feature = "postgres")]
            $crate::repository::diesel_context::DbPool::Postgres(pool) => {
                use $crate::repository::util::to_diesel_error;
                let mut $conn = pool.get().await.map_err(to_diesel_error)?;
                $body
            }
        }
    }};
}

/// Check if a database URL is a PostgreSQL URL.
///
/// Returns true for URLs starting with `postgres://` or `postgresql://`.
pub fn is_postgres_url(url: &str) -> bool {
    url.starts_with("postgres://") || url.starts_with("postgresql://")
}

/// Validate that a PostgreSQL URL can be used with the current build.
///
/// Returns an error if the URL is a PostgreSQL URL but the `postgres` feature is not enabled.
/// Returns Ok(()) otherwise.
pub fn validate_database_url(url: &str) -> Result<(), diesel::result::Error> {
    #[cfg(not(feature = "postgres"))]
    if is_postgres_url(url) {
        return Err(diesel::result::Error::QueryBuilderError(
            "PostgreSQL URL provided but this binary was compiled without PostgreSQL support. \
             Use a build with the 'postgres' feature enabled."
                .into(),
        ));
    }

    let _ = url; // Suppress unused warning when postgres feature is enabled
    Ok(())
}

/// Simple error info wrapper for database errors.
#[derive(Debug)]
pub struct DbErrorInfo(pub String);

impl DatabaseErrorInformation for DbErrorInfo {
    fn message(&self) -> &str {
        &self.0
    }
    fn details(&self) -> Option<&str> {
        None
    }
    fn hint(&self) -> Option<&str> {
        None
    }
    fn table_name(&self) -> Option<&str> {
        None
    }
    fn column_name(&self) -> Option<&str> {
        None
    }
    fn constraint_name(&self) -> Option<&str> {
        None
    }
    fn statement_position(&self) -> Option<i32> {
        None
    }
}

/// Convert any displayable error to a diesel error with proper message.
pub fn to_diesel_error(e: impl std::fmt::Display) -> diesel::result::Error {
    diesel::result::Error::DatabaseError(
        diesel::result::DatabaseErrorKind::Unknown,
        Box::new(DbErrorInfo(e.to_string())),
    )
}

/// Convert a tokio-postgres error to a diesel error, extracting the real message.
///
/// tokio_postgres::Error's Display impl just shows "db error" for database errors,
/// so we need to dig into the source to get the actual message.
#[cfg(feature = "postgres")]
pub fn pg_to_diesel_error(e: tokio_postgres::Error) -> diesel::result::Error {
    // Try to get the database error with detailed message
    let message = if let Some(db_err) = e.as_db_error() {
        format!(
            "{}: {}{}{}",
            db_err.severity(),
            db_err.message(),
            db_err
                .detail()
                .map(|d| format!(" DETAIL: {}", d))
                .unwrap_or_default(),
            db_err
                .hint()
                .map(|h| format!(" HINT: {}", h))
                .unwrap_or_default(),
        )
    } else {
        // Fall back to source chain for non-db errors
        let mut msg = e.to_string();
        let mut source = e.source();
        while let Some(src) = source {
            msg = format!("{}: {}", msg, src);
            source = src.source();
        }
        msg
    };

    diesel::result::Error::DatabaseError(
        diesel::result::DatabaseErrorKind::Unknown,
        Box::new(DbErrorInfo(message)),
    )
}

/// Redact password from a database URL for safe logging/display.
///
/// Transforms `postgres://user:password@host/db` to `postgres://user:***@host/db`
pub fn redact_url_password(url: &str) -> String {
    if !is_postgres_url(url) {
        return url.to_string();
    }

    // Extract prefix and rest
    let (prefix, rest) = if let Some(rest) = url.strip_prefix("postgresql://") {
        ("postgresql://", rest)
    } else if let Some(rest) = url.strip_prefix("postgres://") {
        ("postgres://", rest)
    } else {
        return url.to_string();
    };

    // Find the @ separator - use rfind to handle passwords containing @
    if let Some(at_pos) = rest.rfind('@') {
        let auth = &rest[..at_pos];
        let host_and_rest = &rest[at_pos..];

        // Find the : in the auth section (separates user from password)
        if let Some(colon_pos) = auth.find(':') {
            let user = &auth[..colon_pos];
            return format!("{prefix}{user}:***{host_and_rest}");
        }
    }

    // No password found, return as-is
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_postgres_url() {
        assert!(is_postgres_url("postgres://user:pass@host/db"));
        assert!(is_postgres_url("postgresql://user:pass@host/db"));
        assert!(!is_postgres_url("sqlite:///path/to/db.sqlite"));
        assert!(!is_postgres_url("/path/to/db.sqlite"));
        assert!(!is_postgres_url("file:///path/to/db.sqlite"));
    }

    #[test]
    fn test_validate_database_url() {
        // SQLite URLs are always valid
        assert!(validate_database_url("sqlite:///path/to/db.sqlite").is_ok());
        assert!(validate_database_url("/path/to/db.sqlite").is_ok());

        // PostgreSQL URLs depend on feature flag
        #[cfg(feature = "postgres")]
        {
            assert!(validate_database_url("postgres://user:pass@host/db").is_ok());
            assert!(validate_database_url("postgresql://user:pass@host/db").is_ok());
        }
        #[cfg(not(feature = "postgres"))]
        {
            assert!(validate_database_url("postgres://user:pass@host/db").is_err());
            assert!(validate_database_url("postgresql://user:pass@host/db").is_err());
        }
    }

    #[test]
    fn test_redact_url_password() {
        assert_eq!(
            redact_url_password("postgres://user:secret@host:5432/db"),
            "postgres://user:***@host:5432/db"
        );
        assert_eq!(
            redact_url_password("postgresql://admin:p@ssw0rd@localhost/test"),
            "postgresql://admin:***@localhost/test"
        );
        // No password
        assert_eq!(
            redact_url_password("postgres://user@host/db"),
            "postgres://user@host/db"
        );
        // SQLite path - unchanged
        assert_eq!(
            redact_url_password("/path/to/db.sqlite"),
            "/path/to/db.sqlite"
        );
    }
}
