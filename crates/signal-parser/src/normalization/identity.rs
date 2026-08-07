use sha2::{Digest as _, Sha256};

use super::value::{ComponentId, ContractText, ContractValueError, PipelineId, Sha256Digest};

pub const COMPONENT_CONFIG_DOMAIN: &str = "quant-system/component-config-identity/v1";
pub const RESOLVED_GRAPH_DOMAIN: &str = "quant-system/resolved-graph-identity/v1";
pub const ROUTING_GRAPH_DOMAIN: &str = "quant-system/routing-graph-identity/v1";
pub const PIPELINE_DOMAIN: &str = "quant-system/pipeline-identity/v1";

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum IdentityError {
    #[error(transparent)]
    Value(#[from] ContractValueError),
    #[error("canonical value exceeds the u32 length ceiling")]
    LengthOverflow,
    #[error("invalid semantic version text")]
    InvalidSemanticVersion,
    #[error("canonical floating-point value must be finite")]
    NonFiniteFloat,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CanonicalWriter {
    bytes: Vec<u8>,
}

impl CanonicalWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn bool(&mut self, value: bool) {
        self.bytes.push(u8::from(value));
    }

    pub fn u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn bytes(&mut self, value: &[u8]) -> Result<(), IdentityError> {
        let len = u32::try_from(value.len()).map_err(|_| IdentityError::LengthOverflow)?;
        self.u32(len);
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    pub fn text(&mut self, value: &str) -> Result<(), IdentityError> {
        self.bytes(value.as_bytes())
    }

    pub fn digest(&mut self, value: &Sha256Digest) {
        self.bytes.extend_from_slice(value.as_bytes());
    }

    pub fn finite_f64(&mut self, value: f64) -> Result<(), IdentityError> {
        if !value.is_finite() {
            return Err(IdentityError::NonFiniteFloat);
        }
        let normalized = if value == 0.0 { 0.0 } else { value };
        self.u64(normalized.to_bits());
        Ok(())
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

pub trait CanonicalEncode {
    fn encode(&self, writer: &mut CanonicalWriter) -> Result<(), IdentityError>;
}

pub fn hash_domain(domain: &str, payload: &[u8]) -> Sha256Digest {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update([0]);
    hasher.update(payload);
    Sha256Digest::new(hasher.finalize().into())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SemanticVersion {
    major: u64,
    minor: u64,
    patch: u64,
    prerelease: ContractText<128>,
    build: ContractText<128>,
}

impl SemanticVersion {
    pub fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
            prerelease: ContractText::try_new("", "prerelease")
                .expect("empty version text is valid"),
            build: ContractText::try_new("", "build").expect("empty version text is valid"),
        }
    }

    pub fn with_metadata(
        major: u64,
        minor: u64,
        patch: u64,
        prerelease: impl Into<String>,
        build: impl Into<String>,
    ) -> Result<Self, IdentityError> {
        let prerelease = ContractText::try_new(prerelease, "prerelease")?;
        let build = ContractText::try_new(build, "build")?;
        if !is_version_text(prerelease.as_str()) || !is_version_text(build.as_str()) {
            return Err(IdentityError::InvalidSemanticVersion);
        }
        Ok(Self {
            major,
            minor,
            patch,
            prerelease,
            build,
        })
    }

    pub fn major(&self) -> u64 {
        self.major
    }

    pub fn minor(&self) -> u64 {
        self.minor
    }

    pub fn patch(&self) -> u64 {
        self.patch
    }

    pub fn prerelease(&self) -> &str {
        self.prerelease.as_str()
    }

    pub fn build(&self) -> &str {
        self.build.as_str()
    }
}

fn is_version_text(value: &str) -> bool {
    value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || matches!(character, '.' | '-'))
}

impl CanonicalEncode for SemanticVersion {
    fn encode(&self, writer: &mut CanonicalWriter) -> Result<(), IdentityError> {
        writer.u64(self.major);
        writer.u64(self.minor);
        writer.u64(self.patch);
        writer.text(self.prerelease.as_str())?;
        writer.text(self.build.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ComponentKind {
    Decoder,
    Parser,
    MeaningNormalizer,
    DraftValidator,
    Finalizer,
    PreNormalizedProducer,
}

impl ComponentKind {
    pub fn tag(self) -> u16 {
        match self {
            Self::Decoder => 1,
            Self::Parser => 2,
            Self::MeaningNormalizer => 3,
            Self::DraftValidator => 4,
            Self::Finalizer => 5,
            Self::PreNormalizedProducer => 6,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ComponentConfigSchemaRef(ContractText<128>);

impl ComponentConfigSchemaRef {
    pub fn try_new(value: impl Into<String>) -> Result<Self, ContractValueError> {
        let value = ContractText::try_new(value, "component config schema")?;
        if value.as_str().is_empty() {
            return Err(ContractValueError::Empty {
                field: "component config schema",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ComponentConfigIdentity(Sha256Digest);

impl ComponentConfigIdentity {
    pub fn digest(self) -> Sha256Digest {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResolvedGraphIdentity(Sha256Digest);

impl ResolvedGraphIdentity {
    pub fn from_payload(payload: &[u8]) -> Self {
        Self(hash_domain(RESOLVED_GRAPH_DOMAIN, payload))
    }

    pub fn digest(self) -> Sha256Digest {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RoutingGraphIdentity(Sha256Digest);

impl RoutingGraphIdentity {
    pub fn from_payload(payload: &[u8]) -> Self {
        Self(hash_domain(ROUTING_GRAPH_DOMAIN, payload))
    }

    pub fn digest(self) -> Sha256Digest {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResolvedComponentRef {
    id: ComponentId,
    kind: ComponentKind,
    implementation_version: SemanticVersion,
    contract_version: u32,
    config_identity: ComponentConfigIdentity,
}

impl ResolvedComponentRef {
    pub(crate) fn new(
        id: ComponentId,
        kind: ComponentKind,
        implementation_version: SemanticVersion,
        contract_version: u32,
        config_identity: ComponentConfigIdentity,
    ) -> Self {
        Self {
            id,
            kind,
            implementation_version,
            contract_version,
            config_identity,
        }
    }

    pub fn id(&self) -> &ComponentId {
        &self.id
    }

    pub fn kind(&self) -> ComponentKind {
        self.kind
    }

    pub fn implementation_version(&self) -> &SemanticVersion {
        &self.implementation_version
    }

    pub fn contract_version(&self) -> u32 {
        self.contract_version
    }

    pub fn config_identity(&self) -> ComponentConfigIdentity {
        self.config_identity
    }
}

impl CanonicalEncode for ResolvedComponentRef {
    fn encode(&self, writer: &mut CanonicalWriter) -> Result<(), IdentityError> {
        writer.text(self.id.as_str())?;
        writer.u16(self.kind.tag());
        self.implementation_version.encode(writer)?;
        writer.u32(self.contract_version);
        writer.digest(&self.config_identity.0);
        Ok(())
    }
}

impl AsRef<Sha256Digest> for ComponentConfigIdentity {
    fn as_ref(&self) -> &Sha256Digest {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipelineIdentity {
    digest: Sha256Digest,
    id: PipelineId,
    version: SemanticVersion,
    graph: ResolvedGraphIdentity,
}

impl PipelineIdentity {
    pub fn new(
        id: PipelineId,
        version: SemanticVersion,
        graph: ResolvedGraphIdentity,
    ) -> Result<Self, IdentityError> {
        let mut writer = CanonicalWriter::new();
        writer.text(id.as_str())?;
        version.encode(&mut writer)?;
        writer.digest(&graph.0);
        let digest = hash_domain(PIPELINE_DOMAIN, &writer.into_bytes());
        Ok(Self {
            digest,
            id,
            version,
            graph,
        })
    }

    pub fn digest(&self) -> Sha256Digest {
        self.digest
    }

    pub fn id(&self) -> &PipelineId {
        &self.id
    }

    pub fn version(&self) -> &SemanticVersion {
        &self.version
    }

    pub fn graph(&self) -> ResolvedGraphIdentity {
        self.graph
    }
}

pub fn component_config_identity(
    id: &ComponentId,
    kind: ComponentKind,
    version: &SemanticVersion,
    contract_version: u32,
    schema: &ComponentConfigSchemaRef,
    config_payload: &[u8],
) -> Result<ComponentConfigIdentity, IdentityError> {
    let mut writer = CanonicalWriter::new();
    writer.text(id.as_str())?;
    writer.u16(kind.tag());
    version.encode(&mut writer)?;
    writer.u32(contract_version);
    writer.text(schema.as_str())?;
    writer.bytes(config_payload)?;
    Ok(ComponentConfigIdentity(hash_domain(
        COMPONENT_CONFIG_DOMAIN,
        &writer.into_bytes(),
    )))
}
