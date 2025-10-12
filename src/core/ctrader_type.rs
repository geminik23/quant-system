
use serde::Deserialize;


#[derive(Deserialize, Debug, Clone)]
pub struct CTraderFixConfig{
    pub username: String,
    pub password: String,
    pub server: String,
    pub sendercompid: String,
    pub ssl: bool,
}
