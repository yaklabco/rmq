use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use rmq_auth::password::HashAlgorithm;
use rmq_auth::permissions::Permission;
use rmq_auth::user::UserTag;
use rmq_broker::exchange::ExchangeConfig;
use rmq_broker::queue::QueueConfig;
use rmq_protocol::field_table::FieldTable;

use crate::auth_middleware::authenticate_request;
use crate::state::AppState;

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
    authenticate_request(&headers, &state)?;

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
    let username = authenticate_request(&headers, &state)?;
    let user = state.user_store.get(&username).unwrap();

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
    authenticate_request(&headers, &state)?;

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
    Path(_vhost): Path<String>,
) -> Result<Json<Vec<ExchangeInfo>>, Response> {
    list_exchanges(headers, State(state)).await
}

async fn get_exchange(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((_vhost, name)): Path<(String, String)>,
) -> Result<Json<ExchangeInfo>, Response> {
    authenticate_request(&headers, &state)?;

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
    Path((_vhost, name)): Path<(String, String)>,
    Json(body): Json<ExchangeDeclareBody>,
) -> Result<StatusCode, Response> {
    authenticate_request(&headers, &state)?;

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
    Path((_vhost, name)): Path<(String, String)>,
) -> Result<StatusCode, Response> {
    authenticate_request(&headers, &state)?;

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
    authenticate_request(&headers, &state)?;

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
    Path(_vhost): Path<String>,
) -> Result<Json<Vec<QueueInfo>>, Response> {
    list_queues(headers, State(state)).await
}

async fn get_queue(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((_vhost, name)): Path<(String, String)>,
) -> Result<Json<QueueInfo>, Response> {
    authenticate_request(&headers, &state)?;

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
    Path((_vhost, name)): Path<(String, String)>,
    Json(body): Json<QueueDeclareBody>,
) -> Result<StatusCode, Response> {
    authenticate_request(&headers, &state)?;

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
    Path((_vhost, name)): Path<(String, String)>,
) -> Result<StatusCode, Response> {
    authenticate_request(&headers, &state)?;

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
    authenticate_request(&headers, &state)?;
    // Bindings are stored within exchanges — return empty for now
    // Full implementation requires Exchange trait to expose bindings
    Ok(Json(vec![]))
}

async fn list_exchange_queue_bindings(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((_vhost, _exchange, _queue)): Path<(String, String, String)>,
) -> Result<Json<Vec<BindingInfo>>, Response> {
    authenticate_request(&headers, &state)?;
    Ok(Json(vec![]))
}

async fn create_binding(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((_vhost, exchange, queue)): Path<(String, String, String)>,
    Json(body): Json<BindingBody>,
) -> Result<StatusCode, Response> {
    authenticate_request(&headers, &state)?;

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
    authenticate_request(&headers, &state)?;

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
    authenticate_request(&headers, &state)?;

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
    authenticate_request(&headers, &state)?;

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
    authenticate_request(&headers, &state)?;

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
    authenticate_request(&headers, &state)?;

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
    Path((_vhost, user)): Path<(String, String)>,
) -> Result<Json<PermissionInfo>, Response> {
    authenticate_request(&headers, &state)?;

    let u = state
        .user_store
        .get(&user)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "User not found").into_response())?;

    let perm = u
        .get_permissions("/")
        .ok_or_else(|| (StatusCode::NOT_FOUND, "No permissions").into_response())?
        .clone();

    Ok(Json(PermissionInfo {
        user: u.username,
        vhost: "/".to_string(),
        configure: perm.configure,
        write: perm.write,
        read: perm.read,
    }))
}

async fn set_permissions(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((_vhost, user)): Path<(String, String)>,
    Json(body): Json<PermissionBody>,
) -> Result<StatusCode, Response> {
    authenticate_request(&headers, &state)?;

    state
        .user_store
        .set_permissions(
            &user,
            "/",
            Permission::new(body.configure, body.write, body.read),
        )
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()).into_response())?;

    Ok(StatusCode::NO_CONTENT)
}

async fn delete_permissions(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path((_vhost, user)): Path<(String, String)>,
) -> Result<StatusCode, Response> {
    authenticate_request(&headers, &state)?;

    let mut u = state
        .user_store
        .get(&user)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "User not found").into_response())?;

    u.remove_permissions("/");
    // Note: in a full implementation, we'd save back to the store
    Ok(StatusCode::NO_CONTENT)
}

async fn list_vhosts(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Vec<VHostInfo>>, Response> {
    authenticate_request(&headers, &state)?;
    Ok(Json(vec![VHostInfo {
        name: state.vhost.name().to_string(),
    }]))
}

async fn export_definitions(
    headers: HeaderMap,
    State(state): State<AppState>,
) -> Result<Json<Definitions>, Response> {
    authenticate_request(&headers, &state)?;

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
                password_hash: u.password_hash.clone(),
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
) -> Result<StatusCode, Response> {
    authenticate_request(&headers, &state)?;

    // Import exchanges
    for ex in defs.exchanges {
        let _ = state.vhost.declare_exchange(ExchangeConfig {
            name: ex.name,
            exchange_type: ex.exchange_type,
            durable: ex.durable,
            auto_delete: ex.auto_delete,
            internal: ex.internal,
            arguments: FieldTable::new(),
        });
    }

    // Import queues
    for q in defs.queues {
        let _ = state.vhost.declare_queue(QueueConfig {
            name: q.name,
            durable: q.durable,
            exclusive: q.exclusive,
            auto_delete: q.auto_delete,
            arguments: FieldTable::new(),
        });
    }

    // Import bindings
    for b in defs.bindings {
        if b.destination_type == "queue" {
            let _ = state.vhost.bind_queue(
                &b.destination,
                &b.source,
                &b.routing_key,
                &FieldTable::new(),
            );
        }
    }

    Ok(StatusCode::OK)
}

async fn aliveness_test(
    headers: HeaderMap,
    State(state): State<AppState>,
    Path(_vhost): Path<String>,
) -> Result<Json<serde_json::Value>, Response> {
    authenticate_request(&headers, &state)?;
    Ok(Json(serde_json::json!({"status": "ok"})))
}
