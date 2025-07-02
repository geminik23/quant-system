use async_trait::async_trait;
use mrpc::{Connection, RpcSender, Server};
use rmpv::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::signal;

use crate::QuantError;

pub type RpcHandler = Arc<dyn Fn(Vec<Value>) -> HandlerFuture + Send + Sync>;
pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, QuantError>> + Send>>;

pub struct RpcService {
    handlers: Arc<RwLock<HashMap<String, RpcHandler>>>,
}

impl RpcService {
    pub fn new() -> Self {
        RpcService {
            handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register_handler<F, Fut>(&self, method: &str, handler: F)
    where
        F: Fn(Vec<Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, QuantError>> + Send + 'static,
    {
        log::info!("Registering RPC handler for method: {method}");
        self.handlers.write().unwrap().insert(
            method.to_string(),
            Arc::new(move |params| {
                let fut = handler(params);
                Box::pin(fut) as HandlerFuture
            }) as RpcHandler,
        );
    }
}

impl Default for RpcService {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for RpcService {
    fn clone(&self) -> Self {
        RpcService {
            handlers: self.handlers.clone(),
        }
    }
}

#[async_trait]
impl Connection for RpcService {
    async fn handle_request(
        &self,
        _rpc: RpcSender,
        method: &str,
        params: Vec<Value>,
    ) -> mrpc::Result<Value> {
        let handler_opt = {
            let handlers = self.handlers.read().unwrap();
            handlers.get(method).cloned()
        };
        if let Some(handler) = handler_opt {
            log::info!("Handling RPC method: {method}");
            Ok(handler(params).await.unwrap())
        } else {
            log::error!("Method not found: {method}");
            Ok(Value::Nil)
        }
    }
}

pub async fn start_rpc_server(addr: &str, service: RpcService) -> super::quant_error::Result<()> {
    let server = Server::from_fn(move || service.clone()).tcp(addr).await?;
    log::info!("RPC server listening on {addr}");

    tokio::select! {
        res = server.run() => {
            match res {
                Ok(_) => {
                    log::info!("RPC server stopped gracefully."); 
                    Ok(())
                }
                Err(e) => {log::error!("RPC server error: {e:?}");
                    Err(e.into())
                }
            }
        }
        _ = signal::ctrl_c() => {
            log::info!("Ctrl-C received, shutting down.");
            Ok(())
        }
    }
}


pub async fn run_server_forever(addr: &str, service: RpcService) {
    loop {
        let service_clone = service.clone();
        let server_fut = start_rpc_server(addr, service_clone);
        tokio::select! {
            res = server_fut => {
                if let Err(e) = res {
                    log::error!("Server error: {e:?}, restarting in 1.0 seconds...");
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                } else {
                    log::info!("Server exited cleanly, restarting...");
                }
            }
            _ = signal::ctrl_c() => {
                log::info!("Ctrl-C received, shutting down server loop.");
                break;
            }
        }
    }
}
