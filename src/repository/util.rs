//! Repository utilities.

use diesel::result::DatabaseErrorInformation;
#[cfg(feature = "postgres")]
use std::error::Error;

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
    // Handle postgres:// and postgresql:// URLs
    if let Some(rest) = url
        .strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
    {
        let prefix = if url.starts_with("postgresql://") {
            "postgresql://"
        } else {
            "postgres://"
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
    }

    // No password found or not a postgres URL, return as-is
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

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
