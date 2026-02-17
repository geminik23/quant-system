use serde::{Deserialize, Serialize};

use crate::{core::AlertSet, market_data::AlertId};

// TODO: Remove the legacy commands

#[derive(Debug, Serialize, Deserialize)]
pub enum SystemCommand {
    Ping { uid: String },
    Pong { uid: String },
    SystemMsg { bytes: Vec<u8> },
}

//
// == Alert Commands ==
//
#[derive(Debug, Serialize, Deserialize)]
pub enum AlertCommand {
    SetAlert {
        alert_id: Option<AlertId>,
        symbol: String,
        alert_set: AlertSet,
    },
    RemoveAlert {
        alert_id: AlertId,
    },
}

//
// == Alert Result Commands ==
//

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AlertResultCommand {
    AlertTriggered {
        alert_id: AlertId,
    },
    AlertRemoved {
        alert_id: AlertId,
    },
    AlertUpdated {
        alert_id: AlertId,
        symbol: String,
        alert_set: AlertSet,
    },
}

//
// == Alert Response Commands ==
//

#[derive(Debug, Serialize, Deserialize)]
pub enum AlertResponse {
    AlertSet { alert_id: String },
    AlertRemoved { success: bool },
    Error { message: String },
}
