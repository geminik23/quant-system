use std::collections::HashMap;

// use async_std::sync::RwLock;
// use lazy_static::lazy_static;

pub fn convert_symbol_to_norm(mut symbol: String) -> String {
    symbol = symbol.to_lowercase();
    if let Some(idx) = symbol.find('/') {
        symbol.remove(idx);
    }
    if let Some(idx) = symbol.find('-') {
        symbol.remove(idx);
    }
    symbol
}

pub fn convert_symbol(symbol: &str) -> String {
    let mut symbol = symbol.to_lowercase();
    symbol = match symbol.trim() {
        "spx500" => "us500",
        "nas100" => "us100",
        "ger30" => "de40",
        "ger40" => "de40",
        "de30" => "de40",
        "nasdaq" => "us100",
        "gold" => "xauusd",
        "silver" => "xagusd",
        "oil" | "usoil" => "xtiusd",
        sym => sym,
    }
    .to_string();
    symbol
}

pub struct SymbolInfo {
    pub n: String,
    pub p: u16,
    pub d: u16,
}

impl SymbolInfo {
    pub fn new(name: &str, pip: u16, digit: u16) -> Self {
        Self {
            n: name.into(),
            p: pip,
            d: digit,
        }
    }

    pub fn pip(&self, price: f64, pip: i32) -> f64 {
        let digit = 10i32.pow(self.d as u32);
        let mut d_price = (price * digit as f64).round() as i32;
        let pp = 10i32.pow((self.d - self.p) as u32) * pip;
        d_price = d_price + pp;
        d_price as f64 / digit as f64
    }

    pub fn to_pip(&self, p1: f64, p2: f64) -> f64 {
        let digit = 10i32.pow(self.d as u32);
        let d_digit = 10i32.pow((self.d - self.p) as u32);

        let d_price1 = (p1 * digit as f64).round() as i32;
        let d_price2 = (p2 * digit as f64).round() as i32;
        let d = (d_price1 - d_price2) as f64;
        d / d_digit as f64
    }
}

pub fn symbol_info() -> HashMap<String, SymbolInfo> {
    let mut sym = Vec::new();

    // Crypto currency
    sym.push(SymbolInfo::new("btcusd", 2, 2));
    sym.push(SymbolInfo::new("ethusd", 2, 2));

    // Indice
    sym.push(SymbolInfo::new("us30", 0, 0));
    sym.push(SymbolInfo::new("us100", 0, 0));
    sym.push(SymbolInfo::new("us500", 0, 0));

    // Commodities
    sym.push(SymbolInfo::new("xtiusd", 2, 2));
    sym.push(SymbolInfo::new("xbrusd", 2, 2));
    sym.push(SymbolInfo::new("xngusd", 3, 3));

    // Metals
    sym.push(SymbolInfo::new("xauusd", 1, 2));
    sym.push(SymbolInfo::new("xagusd", 2, 3));

    // Forex
    sym.push(SymbolInfo::new("audcad", 4, 5));
    sym.push(SymbolInfo::new("audchf", 4, 5));
    sym.push(SymbolInfo::new("audjpy", 2, 3));
    sym.push(SymbolInfo::new("audnzd", 4, 5));
    sym.push(SymbolInfo::new("audusd", 4, 5));
    sym.push(SymbolInfo::new("cadchf", 4, 5));
    sym.push(SymbolInfo::new("cadjpy", 2, 3));
    sym.push(SymbolInfo::new("chfjpy", 2, 3));
    sym.push(SymbolInfo::new("chfpln", 4, 5));
    sym.push(SymbolInfo::new("euraud", 4, 5));
    sym.push(SymbolInfo::new("eurcad", 4, 5));
    sym.push(SymbolInfo::new("eurchf", 4, 5));
    sym.push(SymbolInfo::new("eurczk", 2, 3));
    sym.push(SymbolInfo::new("eurdkk", 4, 5));
    sym.push(SymbolInfo::new("eurgbp", 4, 5));
    sym.push(SymbolInfo::new("eurhkd", 4, 5));
    sym.push(SymbolInfo::new("eurhuf", 2, 3));
    sym.push(SymbolInfo::new("eurjpy", 2, 3));
    sym.push(SymbolInfo::new("eurmxn", 4, 5));
    sym.push(SymbolInfo::new("eurnok", 4, 5));
    sym.push(SymbolInfo::new("eurnzd", 4, 5));
    sym.push(SymbolInfo::new("eurpln", 4, 5));
    sym.push(SymbolInfo::new("eurrub", 4, 5));
    sym.push(SymbolInfo::new("eursek", 4, 5));
    sym.push(SymbolInfo::new("eursgd", 4, 5));
    sym.push(SymbolInfo::new("eurtry", 4, 5));
    sym.push(SymbolInfo::new("eurusd", 4, 5));
    sym.push(SymbolInfo::new("eurzar", 4, 5));
    sym.push(SymbolInfo::new("gbpaud", 4, 5));
    sym.push(SymbolInfo::new("gbpcad", 4, 5));
    sym.push(SymbolInfo::new("gbpchf", 4, 5));
    sym.push(SymbolInfo::new("gbphkd", 4, 5));
    sym.push(SymbolInfo::new("gbpjpy", 2, 3));
    sym.push(SymbolInfo::new("gbpnzd", 4, 5));
    sym.push(SymbolInfo::new("gbpusd", 4, 5));
    sym.push(SymbolInfo::new("nzdcad", 4, 5));
    sym.push(SymbolInfo::new("nzdchf", 4, 5));
    sym.push(SymbolInfo::new("nzdjpy", 2, 3));
    sym.push(SymbolInfo::new("nzdusd", 4, 5));
    sym.push(SymbolInfo::new("usdcad", 4, 5));
    sym.push(SymbolInfo::new("usdchf", 4, 5));
    sym.push(SymbolInfo::new("usdcnh", 4, 5));
    sym.push(SymbolInfo::new("usdczk", 3, 4));
    sym.push(SymbolInfo::new("usddkk", 4, 5));
    sym.push(SymbolInfo::new("usdhkd", 3, 4));
    sym.push(SymbolInfo::new("usdhuf", 2, 3));
    sym.push(SymbolInfo::new("usdils", 3, 4));
    sym.push(SymbolInfo::new("usdjpy", 2, 3));
    sym.push(SymbolInfo::new("usdkrw", 1, 2));
    sym.push(SymbolInfo::new("usdmxn", 3, 4));
    sym.push(SymbolInfo::new("usdnok", 4, 5));
    sym.push(SymbolInfo::new("usdpln", 4, 5));
    sym.push(SymbolInfo::new("usdrub", 4, 5));
    sym.push(SymbolInfo::new("usdsek", 4, 5));
    sym.push(SymbolInfo::new("usdsgd", 4, 5));
    sym.push(SymbolInfo::new("usdtry", 4, 5));
    sym.push(SymbolInfo::new("usdzar", 4, 5));

    sym.into_iter().map(|v| (v.n.clone(), v)).collect()
}

// lazy_static! {
//     pub static ref SYMBOL_INFORMATION: Arc<RwLock<HashMap<String, SymbolInfo>>> =
//         Arc::new(RwLock::new(symbol_info()));
//     pub static ref SYMBOLS: HashSet<String> = symbol_info().into_keys().collect();
// }
