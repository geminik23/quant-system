use qs_backtest_api::BacktestClientError;
use thiserror::Error;

/// Stage at which a service catalog probe failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogProbeStage {
    Connect,
    Ping,
    Profiles,
    Symbols,
    Close,
}

impl CatalogProbeStage {
    pub fn user_label(self) -> &'static str {
        match self {
            Self::Connect => "connecting to the backtest service",
            Self::Ping => "checking service health",
            Self::Profiles => "loading management profiles",
            Self::Symbols => "loading market data availability",
            Self::Close => "closing the service connection",
        }
    }
}

/// Provider-neutral failure from a connection and catalog probe.
#[derive(Debug, Clone, Error)]
#[error("failed while {stage_label}: {source}", stage_label = .stage.user_label())]
pub struct CatalogProbeError {
    pub stage: CatalogProbeStage,
    #[source]
    pub source: BacktestClientError,
}

impl CatalogProbeError {
    pub fn new(stage: CatalogProbeStage, source: BacktestClientError) -> Self {
        Self { stage, source }
    }

    pub fn user_message(&self) -> String {
        format!("Could not finish {}.", self.stage.user_label())
    }
}

/// Safe validation errors for the Windows desktop endpoint field.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DesktopEndpointError {
    #[error("enter an endpoint such as tcp://127.0.0.1:41001")]
    InvalidSyntax,
    #[error("the desktop currently supports only tcp:// endpoints")]
    TcpRequired,
    #[error("the desktop currently allows only loopback TCP addresses")]
    LoopbackRequired,
}
