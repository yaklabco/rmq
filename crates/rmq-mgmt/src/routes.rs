use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use rmq_auth::password::HashAlgorithm;
use rmq_auth::permissions::Permission;
use rmq_auth::user::{User, UserTag};
use rmq_broker::exchange::ExchangeConfig;
use rmq_broker::queue::QueueConfig;
use rmq_protocol::field_table::FieldTable;

use crate::auth_middleware::authenticate_request;
use crate::state::AppState;

/// Check that the authenticated user has at least one of the required tags.
/// Returns `403 Forbidden` if the user lacks all required tags.
fn require_tag(user: &User, required: &[UserTag]) -> Result<(), StatusCode> {
    if required.iter().any(|tag| user.has_tag(tag)) {
        Ok(())
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}

/// Tags for read-only endpoints (overview, whoami, list).
const READONLY_TAGS: &[UserTag] = &[
    UserTag::Administrator,
    UserTag::Monitoring,
    UserTag::Management,
];

/// Tags for queue/exchange/binding management.
const MANAGEMENT_TAGS: &[UserTag] = &[UserTag::Administrator, UserTag::Management];

/// Tags for user management / definitions import.
const ADMIN_TAGS: &[UserTag] = &[UserTag::Administrator];

/// Authenticate the request and return the caller's `User`.
fn authenticate_and_get_user(headers: &HeaderMap, state: &AppState) -> Result<User, Response> {
    let username = authenticate_request(headers, state)?;
    state
        .user_store
        .get(&username)
        .ok_or_else(|| (StatusCode::INTERNAL_SERVER_ERROR, "User disappeared").into_response())
}

/// Validate that the vhost path parameter matches the actual vhost name.
/// Returns 404 if the vhost does not exist.
fn validate_vhost(vhost: &str, state: &AppState) -> Result<(), Response> {
    if vhost != state.vhost.name() {
        return Err((StatusCode::NOT_FOUND, "Vhost not found").into_response());
    }
    Ok(())
}

pub fn api_router() -> Router<AppState> {
    Router::new()
        // Overview
        .route("/api/overview", get(get_overview))
        .route("/api/whoami", get(get_whoami))
        // Exchanges
        .route("/api/exchanges", get(list_exchanges))
        .route("/api/exchanges/{vhost}", get(list_vhost_exchanges))
        .route(
            "/api/exchanges/{vhost}/{name}",
            get(get_exchange).put(declare_exchange).delete(delete_exchange),
        )
        // Queues
        .route("/api/queues", get(list_queues))
        .route("/api/queues/{vhost}", get(list_vhost_queues))
        .route(
            "/api/queues/{vhost}/{name}",
            get(get_queue).put(declare_queue).delete(delete_queue),
        )
        // Bindings
        .route("/api/bindings", get(list_bindings))
        .route(
            "/api/bindings/{vhost}/e/{exchange}/q/{queue}",
            get(list_exchange_queue_bindings).post(create_binding),
        )
        // Users
        .route("/api/users", get(list_users))
        .route(
            "/api/users/{name}",
            get(get_user).put(create_user).delete(delete_user),
        )
        // Permissions
        .route("/api/permissions", get(list_permissions))
        .route(
            "/api/permissions/{vhost}/{user}",
            get(get_permissions).put(set_permissions).delete(delete_permissions),
        )
        // VHosts
        .route("/api/vhosts", get(list_vhosts))
        // Definitions
        .route(
            "/api/definitions",
            get(export_definitions).post(import_definitions),
        )
        // Health
        .route("/api/aliveness-test/{vhost}", get(aliveness_test))
        // Body size limit: 10 MB
        .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
        // CORS
        .layer(tower_http::cors::CorsLayer::permissive())
}

// --- Response types ---

#[derive(Serialize)]
struct OverviewResponse {
    product: &'static str,
    version: &'static str,
    exchange_count: usize,
    queue_count: usize,
    user_count: usize,
}

#[derive(Serialize)]
struct ExchangeInfo {
    name: String,
    #[serde(rename = "type")]
    exchange_type: String,
    durable: bool,
    auto_delete: bool,
    internal: bool,
    vhost: String,
}

#[derive(Serialize)]
struct QueueInfo {
    name: String,
    durable: bool,
    exclusive: bool,
    auto_delete: bool,
    messages: u64,
    consumers: u64,
    vhost: String,
}

#[derive(Serialize)]
struct BindingInfo {
    source: String,
    destination: String,
    destination_type: String,
    routing_key: String,
    vhost: String,
}

#[derive(Serialize)]
struct UserInfo {
    name: String,
    tags: String,
}

#[derive(Serialize)]
struct PermissionInfo {
    user: String,
    vhost: String,
    configure: String,
    write: String,
    read: String,
}

#[derive(Serialize)]
struct VHostInfo {
    name: String,
}

#[derive(Serialize)]
struct WhoamiResponse {
    name: String,
    tags: Vec<String>,
}

// --- Request types ---

#[derive(Deserialize)]
struct ExchangeDeclareBody {
    #[serde(rename = "type")]
    exchange_type: String,
    #[serde(default)]
    durable: bool,
    #[serde(default)]
    auto_delete: bool,
    #[serde(default)]
    internal: bool,
}

#[derive(Deserialize)]
struct QueueDeclareBody {
    #[serde(default)]
    durable: bool,
    #[serde(default)]
    exclusive: bool,
    #[serde(default)]
    auto_delete: bool,
}

#[derive(Deserialize)]
struct BindingBody {
    routing_key: String,
}

#[derive(Deserialize)]
struct UserCreateBody {
    password: String,
    #[serde(default)]
    tags: String,
}

#[derive(Deserialize)]
struct PermissionBody {
    configure: String,
    write: String,
    read: String,
}

#[derive(Serialize, Deserialize)]
struct Definitions {
    #[serde(default)]
    exchanges: Vec<ExchangeDef>,
    #[serde(default)]
    queues: Vec<QueueDef>,
    #[serde(default)]
    bindings: Vec<BindingDef>,
    #[serde(default)]
    users: Vec<UserDef>,
    #[serde(default)]
    permissions: Vec<PermissionDef>,
}

#[derive(Serialize, Deserialize)]
struct ExchangeDef {
    name: String,
    #[serde(rename = "type")]
    exchange_type: String,
    #[serde(default)]
    durable: bool,
    #[serde(default)]
    auto_delete: bool,
    #[serde(default)]
    internal: bool,
    vhost: String,
}

#[derive(Serialize, Deserialize)]
struct QueueDef {
    name: String,
    #[serde(default)]
    durable: bool,
    #[serde(default)]
    exclusive: bool,
    #[serde(default)]
    auto_delete: bool,
    vhost: String,
}

#[derive(Serialize, Deserialize)]
struct BindingDef {
    source: String,
    destination: String,
    destination_type: String,
    routing_key: String,
    vhost: String,
}

#[derive(Serialize, Deserialize)]
struct UserDef {
    name: String,
    password_hash: String,
    tags: String,
}

#[derive(Serialize, Deserialize)]
struct PermissionDef {
    user: String,
    vhost: String,
    configure: String,
    write: String,
    read: String,
}

// --- Handlers ---

async fn get_overview(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<OverviewResponse>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, READONLY_TAGS).map_err(IntoResponse::into_response)?;

    Ok(Json(OverviewResponse {
        product: "RMQ",
        version: env!("CARGO_PKG_VERSION"),
        exchange_count: state.vhost.exchange_names().len(),
        queue_count: state.vhost.queue_names().len(),
        user_count: state.user_store.list().len(),
    }))
}

async fn get_whoami(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<WhoamiResponse>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, READONLY_TAGS).map_err(IntoResponse::into_response)?;

    Ok(Json(WhoamiResponse {
        name: user.username,
        tags: user
            .tags
            .iter()
            .map(|t| format!("{t:?}").to_lowercase())
            .collect(),
    }))
}

async fn list_exchanges(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Vec<ExchangeInfo>>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, READONLY_TAGS).map_err(IntoResponse::into_response)?;

    let names = state.vhost.exchange_names();
    let mut exchanges = Vec::new();
    for name in names {
        if let Some(ex) = state.vhost.get_exchange(&name) {
            let ex = ex.read();
            exchanges.push(ExchangeInfo {
                name: ex.name().to_string(),
                exchange_type: ex.exchange_type().to_string(),
                durable: ex.is_durable(),
                auto_delete: ex.is_auto_delete(),
                internal: ex.is_internal(),
                vhost: "/".to_string(),
            });
        }
    }
    Ok(Json(exchanges))
}

async fn list_vhost_exchanges(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(vhost): Path<String>,
) -> Result<Json<Vec<ExchangeInfo>>, Response> {
    validate_vhost(&vhost, &state)?;
    list_exchanges(headers, State(state)).await
}

async fn get_exchange(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> Result<Json<ExchangeInfo>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, READONLY_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;

    let ex = state
        .vhost
        .get_exchange(&name)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Exchange not found").into_response())?;

    let ex = ex.read();
    Ok(Json(ExchangeInfo {
        name: ex.name().to_string(),
        exchange_type: ex.exchange_type().to_string(),
        durable: ex.is_durable(),
        auto_delete: ex.is_auto_delete(),
        internal: ex.is_internal(),
        vhost: "/".to_string(),
    }))
}

async fn declare_exchange(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
    Json(body): Json<ExchangeDeclareBody>,
) -> Result<StatusCode, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, MANAGEMENT_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;

    state
        .vhost
        .declare_exchange(ExchangeConfig {
            name,
            exchange_type: body.exchange_type,
            durable: body.durable,
            auto_delete: body.auto_delete,
            internal: body.internal,
            arguments: FieldTable::new(),
        })
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()).into_response())?;

    Ok(StatusCode::CREATED)
}

async fn delete_exchange(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> Result<StatusCode, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, MANAGEMENT_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;

    state
        .vhost
        .delete_exchange(&name)
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()).into_response())?;

    Ok(StatusCode::NO_CONTENT)
}

async fn list_queues(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Vec<QueueInfo>>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, READONLY_TAGS).map_err(IntoResponse::into_response)?;

    let names = state.vhost.queue_names();
    let mut queues = Vec::new();
    for name in names {
        if let Some(q) = state.vhost.get_queue(&name) {
            queues.push(QueueInfo {
                name: q.name().to_string(),
                durable: q.is_durable(),
                exclusive: q.is_exclusive(),
                auto_delete: q.is_auto_delete(),
                messages: q.message_count(),
                consumers: q.consumer_count(),
                vhost: "/".to_string(),
            });
        }
    }
    Ok(Json(queues))
}

async fn list_vhost_queues(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(vhost): Path<String>,
) -> Result<Json<Vec<QueueInfo>>, Response> {
    validate_vhost(&vhost, &state)?;
    list_queues(headers, State(state)).await
}

async fn get_queue(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> Result<Json<QueueInfo>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, READONLY_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;

    let q = state
        .vhost
        .get_queue(&name)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "Queue not found").into_response())?;

    Ok(Json(QueueInfo {
        name: q.name().to_string(),
        durable: q.is_durable(),
        exclusive: q.is_exclusive(),
        auto_delete: q.is_auto_delete(),
        messages: q.message_count(),
        consumers: q.consumer_count(),
        vhost: "/".to_string(),
    }))
}

async fn declare_queue(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
    Json(body): Json<QueueDeclareBody>,
) -> Result<StatusCode, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, MANAGEMENT_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;

    state
        .vhost
        .declare_queue(QueueConfig {
            name,
            durable: body.durable,
            exclusive: body.exclusive,
            auto_delete: body.auto_delete,
            arguments: FieldTable::new(),
        })
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()).into_response())?;

    Ok(StatusCode::CREATED)
}

async fn delete_queue(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, name)): Path<(String, String)>,
) -> Result<StatusCode, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, MANAGEMENT_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;

    state
        .vhost
        .delete_queue(&name)
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()).into_response())?;

    Ok(StatusCode::NO_CONTENT)
}

async fn list_bindings(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Vec<BindingInfo>>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, READONLY_TAGS).map_err(IntoResponse::into_response)?;
    // Bindings are stored within exchanges — return empty for now
    // Full implementation requires Exchange trait to expose bindings
    Ok(Json(vec![]))
}

async fn list_exchange_queue_bindings(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, _exchange, _queue)): Path<(String, String, String)>,
) -> Result<Json<Vec<BindingInfo>>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, READONLY_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;
    Ok(Json(vec![]))
}

async fn create_binding(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, exchange, queue)): Path<(String, String, String)>,
    Json(body): Json<BindingBody>,
) -> Result<StatusCode, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, MANAGEMENT_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;

    state
        .vhost
        .bind_queue(&queue, &exchange, &body.routing_key, &FieldTable::new())
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()).into_response())?;

    Ok(StatusCode::CREATED)
}

async fn list_users(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Vec<UserInfo>>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, ADMIN_TAGS).map_err(IntoResponse::into_response)?;

    let names = state.user_store.list();
    let users: Vec<_> = names
        .into_iter()
        .filter_map(|name| {
            let user = state.user_store.get(&name)?;
            let tags: Vec<String> = user
                .tags
                .iter()
                .map(|t| format!("{t:?}").to_lowercase())
                .collect();
            Some(UserInfo {
                name: user.username,
                tags: tags.join(","),
            })
        })
        .collect();
    Ok(Json(users))
}

async fn get_user(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<UserInfo>, Response> {
    let caller = authenticate_and_get_user(&headers, &state)?;
    require_tag(&caller, ADMIN_TAGS).map_err(IntoResponse::into_response)?;

    let user = state
        .user_store
        .get(&name)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "User not found").into_response())?;

    let tags: Vec<String> = user
        .tags
        .iter()
        .map(|t| format!("{t:?}").to_lowercase())
        .collect();
    Ok(Json(UserInfo {
        name: user.username,
        tags: tags.join(","),
    }))
}

async fn create_user(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<UserCreateBody>,
) -> Result<StatusCode, Response> {
    let caller = authenticate_and_get_user(&headers, &state)?;
    require_tag(&caller, ADMIN_TAGS).map_err(IntoResponse::into_response)?;

    let tags: Vec<UserTag> = body
        .tags
        .split(',')
        .filter(|s| !s.is_empty())
        .filter_map(|s| match s.trim() {
            "administrator" => Some(UserTag::Administrator),
            "monitoring" => Some(UserTag::Monitoring),
            "policymaker" => Some(UserTag::Policymaker),
            "management" => Some(UserTag::Management),
            _ => None,
        })
        .collect();

    // Try update first, then create
    if state.user_store.get(&name).is_some() {
        state
            .user_store
            .set_password(&name, &body.password, HashAlgorithm::Bcrypt)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
        Ok(StatusCode::NO_CONTENT)
    } else {
        state
            .user_store
            .create(&name, &body.password, HashAlgorithm::Bcrypt, tags)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response())?;
        Ok(StatusCode::CREATED)
    }
}

async fn delete_user(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, Response> {
    let caller = authenticate_and_get_user(&headers, &state)?;
    require_tag(&caller, ADMIN_TAGS).map_err(IntoResponse::into_response)?;

    state
        .user_store
        .delete(&name)
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()).into_response())?;

    Ok(StatusCode::NO_CONTENT)
}

async fn list_permissions(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Vec<PermissionInfo>>, Response> {
    let caller = authenticate_and_get_user(&headers, &state)?;
    require_tag(&caller, ADMIN_TAGS).map_err(IntoResponse::into_response)?;

    let mut perms = Vec::new();
    for username in state.user_store.list() {
        if let Some(user) = state.user_store.get(&username) {
            for (vhost, perm) in &user.permissions {
                perms.push(PermissionInfo {
                    user: username.clone(),
                    vhost: vhost.clone(),
                    configure: perm.configure.clone(),
                    write: perm.write.clone(),
                    read: perm.read.clone(),
                });
            }
        }
    }
    Ok(Json(perms))
}

async fn get_permissions(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, user)): Path<(String, String)>,
) -> Result<Json<PermissionInfo>, Response> {
    let caller = authenticate_and_get_user(&headers, &state)?;
    require_tag(&caller, ADMIN_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;

    let u = state
        .user_store
        .get(&user)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "User not found").into_response())?;

    let perm = u
        .get_permissions(&vhost)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "No permissions").into_response())?
        .clone();

    Ok(Json(PermissionInfo {
        user: u.username,
        vhost,
        configure: perm.configure,
        write: perm.write,
        read: perm.read,
    }))
}

async fn set_permissions(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, user)): Path<(String, String)>,
    Json(body): Json<PermissionBody>,
) -> Result<StatusCode, Response> {
    let caller = authenticate_and_get_user(&headers, &state)?;
    require_tag(&caller, ADMIN_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;

    state
        .user_store
        .set_permissions(
            &user,
            &vhost,
            Permission::new(body.configure, body.write, body.read),
        )
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()).into_response())?;

    Ok(StatusCode::NO_CONTENT)
}

async fn delete_permissions(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((vhost, user)): Path<(String, String)>,
) -> Result<StatusCode, Response> {
    let caller = authenticate_and_get_user(&headers, &state)?;
    require_tag(&caller, ADMIN_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;

    state
        .user_store
        .remove_permissions(&user, &vhost)
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()).into_response())?;

    Ok(StatusCode::NO_CONTENT)
}

async fn list_vhosts(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Vec<VHostInfo>>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, READONLY_TAGS).map_err(IntoResponse::into_response)?;
    Ok(Json(vec![VHostInfo {
        name: state.vhost.name().to_string(),
    }]))
}

async fn export_definitions(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Definitions>, Response> {
    let caller = authenticate_and_get_user(&headers, &state)?;
    require_tag(&caller, READONLY_TAGS).map_err(IntoResponse::into_response)?;

    let is_admin = caller.is_admin();

    let mut exchanges = Vec::new();
    for name in state.vhost.exchange_names() {
        if let Some(ex) = state.vhost.get_exchange(&name) {
            let ex = ex.read();
            exchanges.push(ExchangeDef {
                name: ex.name().to_string(),
                exchange_type: ex.exchange_type().to_string(),
                durable: ex.is_durable(),
                auto_delete: ex.is_auto_delete(),
                internal: ex.is_internal(),
                vhost: "/".to_string(),
            });
        }
    }

    let mut queues = Vec::new();
    for name in state.vhost.queue_names() {
        if let Some(q) = state.vhost.get_queue(&name) {
            queues.push(QueueDef {
                name: q.name().to_string(),
                durable: q.is_durable(),
                exclusive: q.is_exclusive(),
                auto_delete: q.is_auto_delete(),
                vhost: "/".to_string(),
            });
        }
    }

    let mut users = Vec::new();
    for name in state.user_store.list() {
        if let Some(u) = state.user_store.get(&name) {
            let tags: Vec<String> = u
                .tags
                .iter()
                .map(|t| format!("{t:?}").to_lowercase())
                .collect();
            users.push(UserDef {
                name: u.username,
                password_hash: if is_admin {
                    u.password_hash.clone()
                } else {
                    String::new()
                },
                tags: tags.join(","),
            });
        }
    }

    let mut permissions = Vec::new();
    for name in state.user_store.list() {
        if let Some(u) = state.user_store.get(&name) {
            for (vhost, perm) in &u.permissions {
                permissions.push(PermissionDef {
                    user: u.username.clone(),
                    vhost: vhost.clone(),
                    configure: perm.configure.clone(),
                    write: perm.write.clone(),
                    read: perm.read.clone(),
                });
            }
        }
    }

    Ok(Json(Definitions {
        exchanges,
        queues,
        bindings: vec![],
        users,
        permissions,
    }))
}

async fn import_definitions(
    headers: HeaderMap,
    State(state): State<AppState>,
    Json(defs): Json<Definitions>,
) -> Result<Response, Response> {
    let caller = authenticate_and_get_user(&headers, &state)?;
    require_tag(&caller, ADMIN_TAGS).map_err(IntoResponse::into_response)?;

    let mut errors: Vec<String> = Vec::new();

    // Import exchanges
    for ex in defs.exchanges {
        if let Err(e) = state.vhost.declare_exchange(ExchangeConfig {
            name: ex.name.clone(),
            exchange_type: ex.exchange_type,
            durable: ex.durable,
            auto_delete: ex.auto_delete,
            internal: ex.internal,
            arguments: FieldTable::new(),
        }) {
            errors.push(format!("exchange '{}': {}", ex.name, e));
        }
    }

    // Import queues
    for q in defs.queues {
        if let Err(e) = state.vhost.declare_queue(QueueConfig {
            name: q.name.clone(),
            durable: q.durable,
            exclusive: q.exclusive,
            auto_delete: q.auto_delete,
            arguments: FieldTable::new(),
        }) {
            errors.push(format!("queue '{}': {}", q.name, e));
        }
    }

    // Import bindings
    for b in defs.bindings {
        if b.destination_type == "queue" {
            if let Err(e) = state.vhost.bind_queue(
                &b.destination,
                &b.source,
                &b.routing_key,
                &FieldTable::new(),
            ) {
                errors.push(format!(
                    "binding '{}' -> '{}': {}",
                    b.source, b.destination, e
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(StatusCode::OK.into_response())
    } else {
        Ok((
            StatusCode::MULTI_STATUS,
            Json(serde_json::json!({ "errors": errors })),
        )
            .into_response())
    }
}

async fn aliveness_test(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(vhost): Path<String>,
) -> Result<Json<serde_json::Value>, Response> {
    let user = authenticate_and_get_user(&headers, &state)?;
    require_tag(&user, READONLY_TAGS).map_err(IntoResponse::into_response)?;
    validate_vhost(&vhost, &state)?;
    Ok(Json(serde_json::json!({"status": "ok"})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use http_body_util::BodyExt;
    use rmq_auth::password::HashAlgorithm;
    use rmq_auth::user_store::UserStore;
    use rmq_broker::vhost::VHost;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_state(dir: &tempfile::TempDir) -> AppState {
        let vhost = Arc::new(VHost::new("/".to_string(), dir.path().join("vhost")).unwrap());
        let store = Arc::new(
            UserStore::open_with_defaults(dir.path().join("users.json"), "admin", "admin").unwrap(),
        );
        AppState {
            vhost,
            user_store: store,
        }
    }

    fn basic_auth(user: &str, pass: &str) -> String {
        format!("Basic {}", BASE64.encode(format!("{user}:{pass}")))
    }

    fn build_app(state: AppState) -> axum::Router {
        api_router().with_state(state)
    }

    // --- Authorization tests ---

    #[tokio::test]
    async fn test_non_admin_cannot_list_users() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);

        // Create a monitoring-only user
        state
            .user_store
            .create(
                "monitor",
                "pass",
                HashAlgorithm::Plaintext,
                vec![UserTag::Monitoring],
            )
            .unwrap();

        let app = build_app(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/users")
                    .header("authorization", basic_auth("monitor", "pass"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_non_admin_cannot_create_user() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);

        state
            .user_store
            .create(
                "mgmt",
                "pass",
                HashAlgorithm::Plaintext,
                vec![UserTag::Management],
            )
            .unwrap();

        let app = build_app(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/users/newuser")
                    .header("authorization", basic_auth("mgmt", "pass"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"password": "test", "tags": ""}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_admin_can_list_users() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);
        let app = build_app(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/users")
                    .header("authorization", basic_auth("admin", "admin"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_monitoring_user_can_read_overview() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);

        state
            .user_store
            .create(
                "monitor",
                "pass",
                HashAlgorithm::Plaintext,
                vec![UserTag::Monitoring],
            )
            .unwrap();

        let app = build_app(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/overview")
                    .header("authorization", basic_auth("monitor", "pass"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_no_tag_user_forbidden_from_overview() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);

        state
            .user_store
            .create("plain", "pass", HashAlgorithm::Plaintext, vec![])
            .unwrap();

        let app = build_app(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/overview")
                    .header("authorization", basic_auth("plain", "pass"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn test_management_user_cannot_import_definitions() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);

        state
            .user_store
            .create(
                "mgmt",
                "pass",
                HashAlgorithm::Plaintext,
                vec![UserTag::Management],
            )
            .unwrap();

        let app = build_app(state);

        let defs = serde_json::json!({
            "exchanges": [],
            "queues": [],
            "bindings": [],
            "users": [],
            "permissions": []
        });

        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/definitions")
                    .header("authorization", basic_auth("mgmt", "pass"))
                    .header("content-type", "application/json")
                    .body(Body::from(defs.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // --- delete_permissions tests ---

    #[tokio::test]
    async fn test_delete_permissions_persists() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);

        // Create a user with permissions
        state
            .user_store
            .create("alice", "pass", HashAlgorithm::Plaintext, vec![])
            .unwrap();
        state
            .user_store
            .set_permissions(
                "alice",
                "/",
                Permission::new(".*".to_string(), ".*".to_string(), ".*".to_string()),
            )
            .unwrap();

        // Verify alice has permissions
        let alice = state.user_store.get("alice").unwrap();
        assert!(alice.get_permissions("/").is_some());

        let app = build_app(state.clone());

        // Delete permissions via the API
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/permissions/%2F/alice")
                    .header("authorization", basic_auth("admin", "admin"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        // Verify permissions were actually removed from the store
        let alice = state.user_store.get("alice").unwrap();
        assert!(alice.get_permissions("/").is_none());
    }

    // --- Vhost validation tests ---

    #[tokio::test]
    async fn test_invalid_vhost_returns_404() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);
        let app = build_app(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/exchanges/nonexistent")
                    .header("authorization", basic_auth("admin", "admin"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_valid_vhost_accepted() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);
        let app = build_app(state);

        // URL-encoded "/" is "%2F"
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/exchanges/%2F")
                    .header("authorization", basic_auth("admin", "admin"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_aliveness_invalid_vhost_404() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);
        let app = build_app(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/aliveness-test/badhost")
                    .header("authorization", basic_auth("admin", "admin"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    // --- Password hash redaction test ---

    #[tokio::test]
    async fn test_export_definitions_redacts_password_for_non_admin() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);

        state
            .user_store
            .create(
                "monitor",
                "pass",
                HashAlgorithm::Plaintext,
                vec![UserTag::Monitoring],
            )
            .unwrap();

        let app = build_app(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/definitions")
                    .header("authorization", basic_auth("monitor", "pass"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let defs: Definitions = serde_json::from_slice(&body).unwrap();

        // Non-admin should see empty password hashes
        for user in &defs.users {
            assert_eq!(
                user.password_hash, "",
                "password_hash should be redacted for non-admin"
            );
        }
    }

    #[tokio::test]
    async fn test_export_definitions_shows_password_for_admin() {
        let dir = tempfile::TempDir::new().unwrap();
        let state = test_state(&dir);
        let app = build_app(state);

        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/definitions")
                    .header("authorization", basic_auth("admin", "admin"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let defs: Definitions = serde_json::from_slice(&body).unwrap();

        // Admin should see actual password hashes
        for user in &defs.users {
            assert!(
                !user.password_hash.is_empty(),
                "admin should see password hashes"
            );
        }
    }
}
