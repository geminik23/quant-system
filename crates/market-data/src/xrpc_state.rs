use std::collections::HashMap;
use tokio::sync::RwLock;

/// Shared state for managing client connections and alert ownership across xrpc clients.
pub struct XrpcState {
    alerts_owner: RwLock<HashMap<String, usize>>, // alert_id -> client_id
    alert_meta: RwLock<HashMap<String, (String, f64)>>, // alert_id -> (symbol, ref_price)
    client_seq: RwLock<usize>,
}

impl XrpcState {
    pub fn new() -> Self {
        Self {
            alerts_owner: RwLock::new(HashMap::new()),
            alert_meta: RwLock::new(HashMap::new()),
            client_seq: RwLock::new(0),
        }
    }

    pub async fn next_client_id(&self) -> usize {
        let mut w = self.client_seq.write().await;
        *w += 1;
        *w
    }

    pub async fn own_alert(&self, alert_id: &str, client_id: usize) {
        self.alerts_owner
            .write()
            .await
            .insert(alert_id.to_string(), client_id);
    }

    /// Release all alerts owned by a client. Returns the list of released alert IDs
    /// so the caller can also remove them from MarketHandler.
    pub async fn release_alerts_of(&self, client_id: usize) -> Vec<String> {
        let mut owners = self.alerts_owner.write().await;
        let mut meta = self.alert_meta.write().await;
        let ids: Vec<String> = owners
            .iter()
            .filter_map(|(id, owner)| {
                if *owner == client_id {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        for id in &ids {
            owners.remove(id);
            meta.remove(id);
        }
        ids
    }

    pub async fn owner_of(&self, alert_id: &str) -> Option<usize> {
        self.alerts_owner.read().await.get(alert_id).copied()
    }

    pub async fn set_alert_meta(&self, alert_id: &str, symbol: &str, ref_price: f64) {
        self.alert_meta
            .write()
            .await
            .insert(alert_id.to_string(), (symbol.to_string(), ref_price));
    }

    pub async fn take_alert_meta(&self, alert_id: &str) -> Option<(String, f64)> {
        self.alert_meta.write().await.remove(alert_id)
    }

    pub async fn release_alert(&self, alert_id: &str) {
        self.alerts_owner.write().await.remove(alert_id);
        self.alert_meta.write().await.remove(alert_id);
    }
}
