use std::{future::Future, pin::Pin};
use serde::de::DeserializeOwned;
use std::{fs, path::Path};
use crate::{QuantError, Result};
use rmpv::Value;
use tokio::sync::mpsc;

pub fn load_config<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let data = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)?)
}

pub fn setup() {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();
}


// Generic helper for channel-based handlers
pub fn channel_handler<T>(
    tx: mpsc::Sender<T>
) -> impl Fn(Vec<Value>) -> Pin<Box<dyn Future<Output = Result<Value>> + Send>> + Send + Sync + Clone + 'static
where
    T: for<'de> serde::Deserialize<'de> + Send + 'static,
{
    move |params| {
        let tx = tx.clone();
        Box::pin(async move {
            if params.len() != 1 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Expected one parameter",
                )
                .into());
            }
            let buf = params[0].as_slice().ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "Parameter must be binary"))?;
            let cmd: T = rmp_serde::from_slice(buf)?;
            tx.send(cmd).await.map_err(|e| QuantError::SendError(e.to_string()))?;
            Ok(Value::Nil)
        })
    }
}
