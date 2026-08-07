use std::fmt;
use std::sync::Arc;

use crate::ingestion::{
    PayloadEncoding, PayloadSchemaRef, SourceEvent, StructuredPayload, TextPayload,
};

use super::context::{BaseContextSnapshot, PipelineContextRequirements};
use super::diagnostic::ComponentResult;
use super::identity::{
    CanonicalEncode, CanonicalWriter, ComponentConfigSchemaRef, ComponentKind, IdentityError,
    ResolvedComponentRef, SemanticVersion, component_config_identity,
};
use super::signal::{FinalizedSignal, PreNormalizedSignalBatch, SignalDraft};
use super::value::{
    ComponentId, ContractBytes, ContractList, ContractValueError, NonEmptyContractText,
};

pub type DraftBatch = ContractList<SignalDraft, 32>;
pub type MeaningBatch = ContractList<ParsedMeaning, 64>;
pub type FinalizedBatch = ContractList<FinalizedSignal, 32>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyOutputPolicy {
    Ignore,
    Reject,
}

impl EmptyOutputPolicy {
    pub(crate) fn tag(self) -> u16 {
        match self {
            Self::Ignore => 1,
            Self::Reject => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeaningEncoding {
    CanonicalJson,
}

impl MeaningEncoding {
    pub(crate) fn tag(self) -> u16 {
        match self {
            Self::CanonicalJson => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MeaningSchemaRef {
    id: NonEmptyContractText<128>,
    version: u32,
}

impl MeaningSchemaRef {
    pub fn new(id: NonEmptyContractText<128>, version: u32) -> Self {
        Self { id, version }
    }

    pub fn id(&self) -> &NonEmptyContractText<128> {
        &self.id
    }

    pub fn version(&self) -> u32 {
        self.version
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeaningContract {
    schema: MeaningSchemaRef,
    encoding: MeaningEncoding,
}

impl MeaningContract {
    pub fn new(schema: MeaningSchemaRef, encoding: MeaningEncoding) -> Self {
        Self { schema, encoding }
    }

    pub fn schema(&self) -> &MeaningSchemaRef {
        &self.schema
    }

    pub fn encoding(&self) -> MeaningEncoding {
        self.encoding
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedMeaning {
    schema: MeaningSchemaRef,
    encoding: MeaningEncoding,
    bytes: ContractBytes<65536>,
}

impl VersionedMeaning {
    pub fn new(
        schema: MeaningSchemaRef,
        encoding: MeaningEncoding,
        bytes: ContractBytes<65536>,
    ) -> Self {
        Self {
            schema,
            encoding,
            bytes,
        }
    }

    pub fn contract(&self) -> MeaningContract {
        MeaningContract::new(self.schema.clone(), self.encoding)
    }

    pub fn bytes(&self) -> &ContractBytes<65536> {
        &self.bytes
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParsedMeaning {
    Entry(VersionedMeaning),
    Management(VersionedMeaning),
    Informational(VersionedMeaning),
    Extension(VersionedMeaning),
}

impl ParsedMeaning {
    pub fn value(&self) -> &VersionedMeaning {
        match self {
            Self::Entry(value)
            | Self::Management(value)
            | Self::Informational(value)
            | Self::Extension(value) => value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructuredInputCapability {
    schema: PayloadSchemaRef,
    encoding: PayloadEncoding,
}

impl StructuredInputCapability {
    pub fn new(schema: PayloadSchemaRef, encoding: PayloadEncoding) -> Self {
        Self { schema, encoding }
    }

    pub fn schema(&self) -> &PayloadSchemaRef {
        &self.schema
    }

    pub fn encoding(&self) -> PayloadEncoding {
        self.encoding
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentDescriptor {
    id: ComponentId,
    kind: ComponentKind,
    implementation_version: SemanticVersion,
    contract_version: u32,
    config_schema: ComponentConfigSchemaRef,
    requirements: PipelineContextRequirements,
    empty_output: EmptyOutputPolicy,
    structured_inputs: ContractList<StructuredInputCapability, 16>,
    meaning_inputs: ContractList<MeaningContract, 16>,
    meaning_outputs: ContractList<MeaningContract, 16>,
}

impl ComponentDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        id: ComponentId,
        kind: ComponentKind,
        implementation_version: SemanticVersion,
        contract_version: u32,
        config_schema: ComponentConfigSchemaRef,
        requirements: PipelineContextRequirements,
        empty_output: EmptyOutputPolicy,
        structured_inputs: Vec<StructuredInputCapability>,
        meaning_inputs: Vec<MeaningContract>,
        meaning_outputs: Vec<MeaningContract>,
    ) -> Result<Self, ContractValueError> {
        Ok(Self {
            id,
            kind,
            implementation_version,
            contract_version,
            config_schema,
            requirements,
            empty_output,
            structured_inputs: ContractList::try_new(structured_inputs, "structured capabilities")?,
            meaning_inputs: ContractList::try_new(meaning_inputs, "meaning inputs")?,
            meaning_outputs: ContractList::try_new(meaning_outputs, "meaning outputs")?,
        })
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

    pub fn config_schema(&self) -> &ComponentConfigSchemaRef {
        &self.config_schema
    }

    pub fn requirements(&self) -> &PipelineContextRequirements {
        &self.requirements
    }

    pub fn empty_output(&self) -> EmptyOutputPolicy {
        self.empty_output
    }

    pub fn structured_inputs(&self) -> &[StructuredInputCapability] {
        self.structured_inputs.as_slice()
    }

    pub fn meaning_inputs(&self) -> &[MeaningContract] {
        self.meaning_inputs.as_slice()
    }

    pub fn meaning_outputs(&self) -> &[MeaningContract] {
        self.meaning_outputs.as_slice()
    }
}

pub trait CanonicalComponentConfig: Send + Sync {
    fn schema(&self) -> &ComponentConfigSchemaRef;
    fn encode_config(&self, writer: &mut CanonicalWriter) -> Result<(), IdentityError>;
}

#[derive(Debug, Clone)]
pub struct NoConfig {
    schema: ComponentConfigSchemaRef,
}

impl NoConfig {
    pub fn new(schema: ComponentConfigSchemaRef) -> Self {
        Self { schema }
    }
}

impl CanonicalComponentConfig for NoConfig {
    fn schema(&self) -> &ComponentConfigSchemaRef {
        &self.schema
    }

    fn encode_config(&self, _writer: &mut CanonicalWriter) -> Result<(), IdentityError> {
        Ok(())
    }
}

pub trait SignalDecoder: Send + Sync {
    fn decode(
        &self,
        event: &SourceEvent,
        payload: &StructuredPayload,
        context: &BaseContextSnapshot,
    ) -> ComponentResult<DraftBatch>;
}

pub trait MessageParser: Send + Sync {
    fn parse(
        &self,
        event: &SourceEvent,
        payload: &TextPayload,
        context: &BaseContextSnapshot,
    ) -> ComponentResult<MeaningBatch>;
}

pub trait MeaningNormalizer: Send + Sync {
    fn normalize(
        &self,
        meanings: MeaningBatch,
        event: &SourceEvent,
        context: &BaseContextSnapshot,
    ) -> ComponentResult<DraftBatch>;
}

pub trait DraftValidator: Send + Sync {
    fn validate(
        &self,
        drafts: DraftBatch,
        event: &SourceEvent,
        context: &BaseContextSnapshot,
    ) -> ComponentResult<DraftBatch>;
}

pub trait SignalFinalizer: Send + Sync {
    fn finalize(
        &self,
        drafts: DraftBatch,
        event: &SourceEvent,
        context: &BaseContextSnapshot,
    ) -> ComponentResult<FinalizedBatch>;
}

pub trait PreNormalizedProducer: Send + Sync {
    fn produce(
        &self,
        event: &SourceEvent,
        context: &BaseContextSnapshot,
    ) -> ComponentResult<PreNormalizedSignalBatch>;
}

macro_rules! binding_type {
    ($name:ident, $trait_name:ident) => {
        #[derive(Clone)]
        pub struct $name {
            resolved: ResolvedComponentRef,
            descriptor: Arc<ComponentDescriptor>,
            executable: Arc<dyn $trait_name>,
        }

        impl $name {
            pub fn resolved(&self) -> &ResolvedComponentRef {
                &self.resolved
            }

            pub fn descriptor(&self) -> &ComponentDescriptor {
                self.descriptor.as_ref()
            }

            pub(crate) fn executable(&self) -> &dyn $trait_name {
                self.executable.as_ref()
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("resolved", &self.resolved)
                    .field("descriptor", &self.descriptor)
                    .finish_non_exhaustive()
            }
        }
    };
}

binding_type!(DecoderBinding, SignalDecoder);
binding_type!(ParserBinding, MessageParser);
binding_type!(MeaningNormalizerBinding, MeaningNormalizer);
binding_type!(DraftValidatorBinding, DraftValidator);
binding_type!(FinalizerBinding, SignalFinalizer);
binding_type!(PreNormalizedProducerBinding, PreNormalizedProducer);

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ComponentBindError {
    #[error("component descriptor kind does not match the requested binding")]
    KindMismatch,
    #[error("component configuration schema does not match the descriptor")]
    ConfigSchemaMismatch,
    #[error(transparent)]
    Identity(#[from] IdentityError),
    #[error("component construction failed: {0}")]
    Build(String),
}

fn resolve_component<C: CanonicalComponentConfig>(
    descriptor: &ComponentDescriptor,
    expected: ComponentKind,
    config: &C,
) -> Result<ResolvedComponentRef, ComponentBindError> {
    if descriptor.kind() != expected {
        return Err(ComponentBindError::KindMismatch);
    }
    if descriptor.config_schema() != config.schema() {
        return Err(ComponentBindError::ConfigSchemaMismatch);
    }
    let mut writer = CanonicalWriter::new();
    config.encode_config(&mut writer)?;
    let identity = component_config_identity(
        descriptor.id(),
        descriptor.kind(),
        descriptor.implementation_version(),
        descriptor.contract_version(),
        descriptor.config_schema(),
        &writer.into_bytes(),
    )?;
    Ok(ResolvedComponentRef::new(
        descriptor.id().clone(),
        descriptor.kind(),
        descriptor.implementation_version().clone(),
        descriptor.contract_version(),
        identity,
    ))
}

macro_rules! bind_function {
    ($function:ident, $binding:ident, $trait_name:ident, $kind:expr) => {
        pub fn $function<C, E, F>(
            descriptor: ComponentDescriptor,
            config: &C,
            build: F,
        ) -> Result<$binding, ComponentBindError>
        where
            C: CanonicalComponentConfig,
            E: $trait_name + 'static,
            F: FnOnce(&C) -> Result<E, String>,
        {
            let resolved = resolve_component(&descriptor, $kind, config)?;
            let executable = build(config).map_err(ComponentBindError::Build)?;
            Ok($binding {
                resolved,
                descriptor: Arc::new(descriptor),
                executable: Arc::new(executable),
            })
        }
    };
}

bind_function!(
    bind_decoder,
    DecoderBinding,
    SignalDecoder,
    ComponentKind::Decoder
);
bind_function!(
    bind_parser,
    ParserBinding,
    MessageParser,
    ComponentKind::Parser
);
bind_function!(
    bind_meaning_normalizer,
    MeaningNormalizerBinding,
    MeaningNormalizer,
    ComponentKind::MeaningNormalizer
);
bind_function!(
    bind_draft_validator,
    DraftValidatorBinding,
    DraftValidator,
    ComponentKind::DraftValidator
);
bind_function!(
    bind_finalizer,
    FinalizerBinding,
    SignalFinalizer,
    ComponentKind::Finalizer
);
bind_function!(
    bind_pre_normalized_producer,
    PreNormalizedProducerBinding,
    PreNormalizedProducer,
    ComponentKind::PreNormalizedProducer
);

pub(crate) fn encode_descriptor_ref(
    descriptor: &ComponentDescriptor,
    resolved: &ResolvedComponentRef,
    writer: &mut CanonicalWriter,
) -> Result<(), IdentityError> {
    resolved.encode(writer)?;
    writer.u16(descriptor.empty_output().tag());
    let requirements = descriptor.requirements();
    match requirements.history() {
        Some(history) => {
            writer.bool(true);
            writer.u32(history.maximum_items().get());
            writer.u64(history.maximum_bytes().get());
            writer.bool(history.include_payload());
            writer.bool(history.include_adapter_evidence());
        }
        None => writer.bool(false),
    }
    writer.u16(requirements.parent().tag());
    writer.u32(requirements.maximum_items().get());
    writer.u64(requirements.maximum_bytes().get());
    Ok(())
}
