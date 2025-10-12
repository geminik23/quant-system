use crate::Result;
use serde::de::DeserializeOwned;
use std::{fs, path::Path};

pub fn load_config<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let data = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&data)?)
}

pub fn setup() {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();
}
