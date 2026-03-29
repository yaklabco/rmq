use std::sync::Arc;

use rmq_auth::user_store::UserStore;
use rmq_broker::vhost::VHost;

/// Shared application state for the management API.
#[derive(Clone)]
pub struct AppState {
    pub vhost: Arc<VHost>,
    pub user_store: Arc<UserStore>,
}
