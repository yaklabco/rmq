use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;

use crate::state::AppState;

/// Extract and validate Basic auth credentials from the request.
/// Returns the authenticated username or an error response.
pub fn authenticate_request(
    headers: &HeaderMap,
    state: &AppState,
) -> Result<String, Response> {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                [("WWW-Authenticate", "Basic realm=\"RMQ Management\"")],
                "Authentication required",
            )
                .into_response()
        })?;

    if !auth_header.starts_with("Basic ") {
        return Err((StatusCode::UNAUTHORIZED, "Invalid auth scheme").into_response());
    }

    let decoded = BASE64
        .decode(&auth_header[6..])
        .map_err(|_| (StatusCode::UNAUTHORIZED, "Invalid base64").into_response())?;

    let credentials = String::from_utf8(decoded)
        .map_err(|_| (StatusCode::UNAUTHORIZED, "Invalid UTF-8").into_response())?;

    let (username, password) = credentials
        .split_once(':')
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, "Invalid credentials format").into_response())?;

    state
        .user_store
        .authenticate(username, password)
        .map_err(|_| (StatusCode::UNAUTHORIZED, "Invalid credentials").into_response())?;

    Ok(username.to_string())
}
