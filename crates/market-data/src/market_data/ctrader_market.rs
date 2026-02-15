use cfix::{
    MarketClient, TradeClient,
    types::{SpotPrice, SymbolInformation},
};
use std::collections::{HashMap, HashSet};

use crate::{QuantError, Result, core::ctrader_type::CTraderFixConfig};

use super::utils::convert_symbol_to_norm;

//
// == Symbol Information ==
//

pub struct SymbolInfo {
    pub id2name: HashMap<u32, String>,
    pub name2id: HashMap<String, u32>,
    pub infos: HashMap<u32, SymbolInformation>,
}

impl SymbolInfo {
    pub fn parse_symbol_infos(&mut self, infos: Vec<SymbolInformation>) {
        for mut si in infos.into_iter() {
            si.name = convert_symbol_to_norm(si.name);
            // let sym = convert_symbol_to_norm(si.name.clone());
            self.id2name.insert(si.id, si.name.clone());
            self.name2id.insert(si.name.clone(), si.id);
            self.infos.insert(si.id, si);
        }
    }
}

//
// == CTrader Market ==
//

pub struct CTraderMarket {
    api_info: CTraderFixConfig,
    pub client: MarketClient,
    symbol_info: SymbolInfo,
}

impl CTraderMarket {
    pub fn new(api_info: CTraderFixConfig) -> Self {
        let client = MarketClient::new(
            api_info.server.clone(),
            api_info.username.clone(),
            api_info.password.clone(),
            api_info.sendercompid.clone(),
            None,
        );
        Self {
            client,
            api_info,
            symbol_info: SymbolInfo {
                id2name: HashMap::new(),
                name2id: HashMap::new(),
                infos: HashMap::new(),
            },
        }
    }

    pub async fn get_symbol_str2id(&self) -> HashMap<String, u32> {
        self.symbol_info.name2id.clone()
    }

    pub async fn get_price_of(&self, symbol: &str) -> Result<SpotPrice> {
        let sid = self
            .symbol_info
            .name2id
            .get(symbol)
            .ok_or(QuantError::SymbolNotFound(symbol.into()))?;
        self.client
            // .read()
            // .await
            .price_of(*sid)
            .await
            .map_err(Into::into)
    }

    pub fn get_all_symbol_ids(&self) -> Vec<u32> {
        self.symbol_info.infos.keys().copied().collect()
    }

    /// Check the connection.
    /// If disconneted then re-initialize and receive symbol information again.
    pub async fn check_connection(&mut self) -> Result<()> {
        if !self.client.is_connected() {
            // if !self.quote.read().await.is_connected() {
            self.initialize(true).await?
        }
        Ok(())
    }

    pub async fn initialize(&mut self, disonnect: bool) -> Result<()> {
        if disonnect {
            if let Err(err) = self.client.disconnect().await {
                tracing::error!(
                    "[CtraderMakret] Failed to disconnect on intiailize method - {err:?}",
                );
            }
        }

        self.client.connect().await?;
        // self.quote.write().await.connect().await?;
        // with trade client.get the sec list.
        // FIXME later

        let mut trade = TradeClient::new(
            self.api_info.server.clone(),
            self.api_info.username.clone(),
            self.api_info.password.clone(),
            self.api_info.sendercompid.clone(),
            None,
        );

        {
            // Receive the symbol informations
            trade.connect().await?;
            let data = trade
                .fetch_security_list()
                .await
                .expect("CtraderMarket::initialize - the result of fetch_security_list");
            self.symbol_info.parse_symbol_infos(data);
            tracing::info!("Subscribe market datas");

            trade.disconnect().await?;
        }

        {
            tracing::info!("Start market subscription...");
            let symbols = self
                .get_all_symbol_ids()
                .into_iter()
                .collect::<HashSet<_>>();
            for symbol_id in symbols.iter() {
                self.client.subscribe_spot(*symbol_id).await?;
                // let _ = self.quote.write().await.subscribe_spot(symbol_id).await?;
            }

            // let subscription_list = self.client.spot_subscription_list().await;
            // let rejected_list = self.client.spot_subscription_list().await;
        }

        tracing::info!("Success to subscribe market datas");

        Ok(())
    }
}
