//! Shared infrastructure for cloud API OCR backends (Groq, Gemini).
//!
//! Provides common helpers for image encoding, rate limiting,
//! retry logic, and async-to-sync bridging.

use std::future::Future;
use std::path::Path;
use std::time::Duration;

use base64::Engine;
use tokio::runtime::Handle;
use tracing::{debug, warn};

use super::backend::{OcrBackendType, OcrError};
use foiacquire::http_client::HttpResponse;
use foiacquire::rate_limit::{backoff_delay, get_delay_from_env, parse_retry_after};

/// Maximum retry attempts on rate limit (429) errors.
const MAX_RETRIES: u32 = 5;

/// Shared OCR prompt for vision API backends.
pub const VISION_OCR_PROMPT: &str = "Extract all text from this image. Return only the extracted text, preserving the original layout and formatting as much as possible. Do not add any explanations or commentary.";

/// Read an image file and encode it as base64, returning (base64_data, mime_type).
pub fn encode_image_base64(image_path: &Path) -> Result<(String, &'static str), OcrError> {
    let image_bytes = std::fs::read(image_path)?;
    let base64_data = base64::engine::general_purpose::STANDARD.encode(&image_bytes);

    let mime_type = if image_path.extension().is_some_and(|e| e == "png") {
        "image/png"
    } else {
        "image/jpeg"
    };

    Ok((base64_data, mime_type))
}

/// Apply a configurable rate-limiting delay before an API request.
pub async fn apply_rate_delay(env_var: &str, default_ms: u64, backend_name: &str) {
    let delay = get_delay_from_env(env_var, default_ms);
    if delay > Duration::ZERO {
        debug!("{}: waiting {:?} before request", backend_name, delay);
        tokio::time::sleep(delay).await;
    }
}

/// Retry an API request on 429 (rate limited) responses with exponential backoff.
///
/// Returns the first non-429 response. If all retries are exhausted,
/// returns `OcrError::RateLimited`.
pub async fn retry_on_rate_limit<F, Fut>(
    backend_type: OcrBackendType,
    make_request: F,
) -> Result<HttpResponse, OcrError>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<HttpResponse, OcrError>>,
{
    let mut attempt = 0;
    loop {
        let response = make_request().await?;

        if response.status.as_u16() != 429 {
            return Ok(response);
        }

        let retry_after = response.headers.get("retry-after").map(|s| s.as_str());
        let retry_after_secs = retry_after.and_then(|s| s.parse::<u64>().ok());

        if attempt >= MAX_RETRIES {
            return Err(OcrError::RateLimited {
                backend: backend_type,
                retry_after_secs,
            });
        }

        let wait = parse_retry_after(retry_after).unwrap_or_else(|| backoff_delay(attempt, 1000));

        warn!(
            "{} rate limited (attempt {}), waiting {:?}",
            backend_type,
            attempt + 1,
            wait
        );
        tokio::time::sleep(wait).await;
        attempt += 1;
    }
}

/// Block on an async future using the current tokio runtime handle.
pub fn block_on_async<F, T>(backend_name: &str, future: F) -> Result<T, OcrError>
where
    F: Future<Output = Result<T, OcrError>>,
{
    let handle = Handle::try_current().map_err(|_| {
        OcrError::OcrFailed(format!(
            "No tokio runtime available for {} OCR",
            backend_name
        ))
    })?;
    handle.block_on(future)
}
