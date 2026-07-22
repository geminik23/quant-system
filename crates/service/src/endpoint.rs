use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

const MAX_ENDPOINT_LEN: usize = 4096;
const MAX_LOGICAL_NAME_LEN: usize = 255;

/// Supported endpoint schemes at the framework service boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EndpointScheme {
    Channel,
    SharedMemory,
    Unix,
    Tcp,
    Custom(String),
}

impl EndpointScheme {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Channel => "channel",
            Self::SharedMemory => "shm",
            Self::Unix => "unix",
            Self::Tcp => "tcp",
            Self::Custom(value) => value,
        }
    }
}

/// A validated service endpoint independent of the selected RPC provider.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServiceEndpoint {
    scheme: EndpointScheme,
    address: String,
}

impl ServiceEndpoint {
    pub fn new(
        scheme: EndpointScheme,
        address: impl Into<String>,
    ) -> Result<Self, ServiceEndpointError> {
        let endpoint = Self {
            scheme,
            address: address.into(),
        };
        endpoint.validate()?;
        Ok(endpoint)
    }

    pub fn shared_memory(name: impl Into<String>) -> Result<Self, ServiceEndpointError> {
        Self::new(EndpointScheme::SharedMemory, name)
    }

    pub fn scheme(&self) -> &EndpointScheme {
        &self.scheme
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn socket_addr(&self) -> Result<SocketAddr, ServiceEndpointError> {
        if self.scheme != EndpointScheme::Tcp {
            return Err(ServiceEndpointError::WrongScheme("tcp"));
        }
        self.address
            .parse()
            .map_err(|_| ServiceEndpointError::InvalidTcpAddress(self.address.clone()))
    }

    pub fn unix_path(&self) -> Result<&Path, ServiceEndpointError> {
        if self.scheme != EndpointScheme::Unix {
            return Err(ServiceEndpointError::WrongScheme("unix"));
        }
        Ok(Path::new(&self.address))
    }

    pub fn is_tcp_loopback(&self) -> bool {
        self.socket_addr()
            .map(|address| address.ip().is_loopback())
            .unwrap_or(false)
    }

    pub fn tcp_ip(&self) -> Option<IpAddr> {
        self.socket_addr().ok().map(|address| address.ip())
    }

    pub fn redacted(&self) -> String {
        match self.scheme {
            EndpointScheme::Tcp => self
                .socket_addr()
                .map(|address| format!("tcp://{}:{}", address.ip(), address.port()))
                .unwrap_or_else(|_| "tcp://<invalid>".to_string()),
            _ => self.to_string(),
        }
    }

    fn validate(&self) -> Result<(), ServiceEndpointError> {
        if self.address.is_empty() {
            return Err(ServiceEndpointError::EmptyAddress);
        }
        if self.address.len() > MAX_ENDPOINT_LEN {
            return Err(ServiceEndpointError::TooLong);
        }
        if self.address.contains('?') || self.address.contains('#') || self.address.contains('@') {
            return Err(ServiceEndpointError::CredentialsOrQueryNotAllowed);
        }
        match &self.scheme {
            EndpointScheme::Channel | EndpointScheme::SharedMemory => {
                if self.address.len() > MAX_LOGICAL_NAME_LEN
                    || !self.address.chars().all(|character| {
                        character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.')
                    })
                {
                    return Err(ServiceEndpointError::InvalidLogicalName(
                        self.address.clone(),
                    ));
                }
            }
            EndpointScheme::Unix => {
                if !Path::new(&self.address).is_absolute() {
                    return Err(ServiceEndpointError::UnixPathMustBeAbsolute);
                }
            }
            EndpointScheme::Tcp => {
                self.address
                    .parse::<SocketAddr>()
                    .map_err(|_| ServiceEndpointError::InvalidTcpAddress(self.address.clone()))?;
            }
            EndpointScheme::Custom(scheme) => {
                if scheme.is_empty()
                    || !scheme.chars().all(|character| {
                        character.is_ascii_lowercase()
                            || character.is_ascii_digit()
                            || matches!(character, '+' | '-' | '.')
                    })
                {
                    return Err(ServiceEndpointError::InvalidScheme(scheme.clone()));
                }
            }
        }
        Ok(())
    }
}

impl fmt::Display for ServiceEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.scheme {
            EndpointScheme::Unix => write!(formatter, "unix://{}", self.address),
            _ => write!(formatter, "{}://{}", self.scheme.as_str(), self.address),
        }
    }
}

impl FromStr for ServiceEndpoint {
    type Err = ServiceEndpointError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() > MAX_ENDPOINT_LEN {
            return Err(ServiceEndpointError::TooLong);
        }
        let (scheme, address) = value
            .split_once("://")
            .ok_or(ServiceEndpointError::MissingScheme)?;
        let scheme = match scheme {
            "channel" => EndpointScheme::Channel,
            "shm" => EndpointScheme::SharedMemory,
            "unix" => EndpointScheme::Unix,
            "tcp" => EndpointScheme::Tcp,
            other => EndpointScheme::Custom(other.to_string()),
        };
        Self::new(scheme, address)
    }
}

impl Serialize for ServiceEndpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ServiceEndpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ServiceEndpointError {
    #[error("service endpoint is missing a scheme such as shm://, unix://, or tcp://")]
    MissingScheme,
    #[error("service endpoint address is empty")]
    EmptyAddress,
    #[error("service endpoint exceeds the maximum length")]
    TooLong,
    #[error("credentials, query strings, and fragments are not allowed in service endpoints")]
    CredentialsOrQueryNotAllowed,
    #[error("invalid logical service endpoint name '{0}'")]
    InvalidLogicalName(String),
    #[error("Unix service endpoint paths must be absolute")]
    UnixPathMustBeAbsolute,
    #[error("invalid TCP socket address '{0}'; use an explicit IP address and port")]
    InvalidTcpAddress(String),
    #[error("invalid custom endpoint scheme '{0}'")]
    InvalidScheme(String),
    #[error("operation requires a {0} endpoint")]
    WrongScheme(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_round_trips() {
        for value in [
            "channel://contract-test",
            "shm://backtest",
            "unix:///tmp/quant-system.sock",
            "tcp://127.0.0.1:41001",
        ] {
            let parsed: ServiceEndpoint = value.parse().unwrap();
            assert_eq!(parsed.to_string(), value);
        }
    }

    #[test]
    fn rejects_unsafe_or_ambiguous_addresses() {
        assert!("backtest".parse::<ServiceEndpoint>().is_err());
        assert!("unix://relative.sock".parse::<ServiceEndpoint>().is_err());
        assert!("tcp://localhost:1234".parse::<ServiceEndpoint>().is_err());
        assert!(
            "shm://name?token=secret"
                .parse::<ServiceEndpoint>()
                .is_err()
        );
    }
}
