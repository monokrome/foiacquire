//! Static file serving handlers.

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};

use super::super::assets;
use super::super::AppState;

/// Serve a document file.
pub async fn serve_file(State(state): State<AppState>, Path(path): Path<String>) -> Response {
    let canonical_docs_dir = match state.documents_dir.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Server configuration error",
            )
                .into_response();
        }
    };

    if path.contains("..") || path.starts_with('/') {
        return (StatusCode::NOT_FOUND, "File not found").into_response();
    }

    let file_path = canonical_docs_dir.join(&path);

    let canonical_file = match file_path.canonicalize() {
        Ok(p) => p,
        Err(_) => {
            return (StatusCode::NOT_FOUND, "File not found").into_response();
        }
    };

    if !canonical_file.starts_with(&canonical_docs_dir) {
        return (StatusCode::NOT_FOUND, "File not found").into_response();
    }

    let content = match tokio::fs::read(&canonical_file).await {
        Ok(c) => c,
        Err(_) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, "Failed to read file").into_response();
        }
    };

    let mime = mime_guess::from_path(&canonical_file)
        .first_or_octet_stream()
        .to_string();

    ([(header::CONTENT_TYPE, mime)], content).into_response()
}

/// Serve CSS.
pub async fn serve_css() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css")], assets::CSS)
}

/// Serve JavaScript.
pub async fn serve_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/javascript")],
        assets::JS,
    )
}
