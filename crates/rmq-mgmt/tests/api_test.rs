use std::net::SocketAddr;
use std::sync::Arc;

use reqwest::StatusCode;

use rmq_auth::user_store::UserStore;
use rmq_broker::vhost::VHost;
use rmq_mgmt::routes::api_router;
use rmq_mgmt::state::AppState;

async fn start_api() -> (String, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let vhost_dir = dir.path().join("vhosts").join("default");
    let vhost = Arc::new(VHost::new("/".into(), &vhost_dir).unwrap());

    let users_path = dir.path().join("users.json");
    let user_store = Arc::new(
        UserStore::open_with_defaults(&users_path, "guest", "guest").unwrap(),
    );

    let state = AppState {
        vhost,
        user_store,
    };
    let app = api_router().with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    (format!("http://{addr}"), dir)
}

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

fn auth(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    req.basic_auth("guest", Some("guest"))
}

#[tokio::test]
async fn test_overview() {
    let (base, _dir) = start_api().await;
    let resp = auth(client().get(format!("{base}/api/overview")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["product"], "RMQ");
    assert!(body["exchange_count"].as_u64().unwrap() >= 4); // default exchanges
}

#[tokio::test]
async fn test_unauthorized() {
    let (base, _dir) = start_api().await;
    let resp = client()
        .get(format!("{base}/api/overview"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_wrong_password() {
    let (base, _dir) = start_api().await;
    let resp = client()
        .get(format!("{base}/api/overview"))
        .basic_auth("guest", Some("wrong"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_whoami() {
    let (base, _dir) = start_api().await;
    let resp = auth(client().get(format!("{base}/api/whoami")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "guest");
}

#[tokio::test]
async fn test_list_exchanges() {
    let (base, _dir) = start_api().await;
    let resp = auth(client().get(format!("{base}/api/exchanges")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Vec<serde_json::Value> = resp.json().await.unwrap();
    // Should have default exchanges
    let names: Vec<String> = body.iter().map(|e| e["name"].as_str().unwrap().to_string()).collect();
    assert!(names.contains(&"amq.direct".to_string()));
    assert!(names.contains(&"amq.fanout".to_string()));
    assert!(names.contains(&"amq.topic".to_string()));
}

#[tokio::test]
async fn test_declare_and_get_queue() {
    let (base, _dir) = start_api().await;

    // Declare queue via API
    let resp = auth(client().put(format!("{base}/api/queues/%2F/api-queue")))
        .json(&serde_json::json!({"durable": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Get queue
    let resp = auth(client().get(format!("{base}/api/queues/%2F/api-queue")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "api-queue");
    assert_eq!(body["durable"], true);
    assert_eq!(body["messages"], 0);

    // Delete queue
    let resp = auth(client().delete(format!("{base}/api/queues/%2F/api-queue")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Verify gone
    let resp = auth(client().get(format!("{base}/api/queues/%2F/api-queue")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_declare_exchange_via_api() {
    let (base, _dir) = start_api().await;

    let resp = auth(client().put(format!("{base}/api/exchanges/%2F/my-topic")))
        .json(&serde_json::json!({"type": "topic", "durable": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = auth(client().get(format!("{base}/api/exchanges/%2F/my-topic")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "my-topic");
    assert_eq!(body["type"], "topic");
}

#[tokio::test]
async fn test_user_crud() {
    let (base, _dir) = start_api().await;

    // Create user
    let resp = auth(client().put(format!("{base}/api/users/alice")))
        .json(&serde_json::json!({"password": "secret", "tags": "monitoring"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Get user
    let resp = auth(client().get(format!("{base}/api/users/alice")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["name"], "alice");

    // List users — should include guest and alice
    let resp = auth(client().get(format!("{base}/api/users")))
        .send()
        .await
        .unwrap();
    let users: Vec<serde_json::Value> = resp.json().await.unwrap();
    let names: Vec<String> = users.iter().map(|u| u["name"].as_str().unwrap().to_string()).collect();
    assert!(names.contains(&"guest".to_string()));
    assert!(names.contains(&"alice".to_string()));

    // Delete user
    let resp = auth(client().delete(format!("{base}/api/users/alice")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn test_vhosts() {
    let (base, _dir) = start_api().await;
    let resp = auth(client().get(format!("{base}/api/vhosts")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(body.len(), 1);
    assert_eq!(body[0]["name"], "/");
}

#[tokio::test]
async fn test_definitions_export() {
    let (base, _dir) = start_api().await;
    let resp = auth(client().get(format!("{base}/api/definitions")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["exchanges"].as_array().unwrap().len() >= 4);
    assert!(body["users"].as_array().unwrap().len() >= 1);
}

#[tokio::test]
async fn test_aliveness() {
    let (base, _dir) = start_api().await;
    let resp = auth(client().get(format!("{base}/api/aliveness-test/%2F")))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn test_create_binding_via_api() {
    let (base, _dir) = start_api().await;

    // Create queue first
    auth(client().put(format!("{base}/api/queues/%2F/bind-q")))
        .json(&serde_json::json!({"durable": false}))
        .send()
        .await
        .unwrap();

    // Create binding
    let resp = auth(client().post(format!(
        "{base}/api/bindings/%2F/e/amq.direct/q/bind-q"
    )))
    .json(&serde_json::json!({"routing_key": "my-key"}))
    .send()
    .await
    .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}
