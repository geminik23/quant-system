use base64::{Engine as _, engine::general_purpose::STANDARD};
use qs_backtest_api::{
    BacktestResultMsg, GetResultArtifactChunkResponse, RESULT_FORMAT_VERSION, ResultArtifactRefMsg,
};
use qs_backtest_client::{ArtifactDownload, ResultIoLimits};
use sha2::{Digest, Sha256};

#[test]
fn artifact_chunks_verify_identity_offsets_length_and_digest() {
    let directory = tempfile::tempdir().unwrap();
    let payload = serde_json::to_vec(&BacktestResultMsg::default()).unwrap();
    let reference = reference(&payload, 17);
    let mut download = ArtifactDownload::start(
        reference.clone(),
        directory.path(),
        ResultIoLimits::default(),
    )
    .unwrap();
    for (index, bytes) in payload.chunks(17).enumerate() {
        let offset = (index * 17) as u64;
        let eof = offset + bytes.len() as u64 == payload.len() as u64;
        assert_eq!(download.next_request().offset, offset);
        let done = download
            .accept(GetResultArtifactChunkResponse {
                success: true,
                artifact_id: reference.artifact_id.clone(),
                offset,
                data_base64: STANDARD.encode(bytes),
                eof,
                error: None,
            })
            .unwrap();
        assert_eq!(done, eof);
    }
    let verified = download.finish(ResultIoLimits::default()).unwrap();
    assert_eq!(verified.byte_len, payload.len() as u64);
    assert_eq!(verified.result.total_trades, 0);
}

#[test]
fn artifact_failure_does_not_advance_offset() {
    let directory = tempfile::tempdir().unwrap();
    let payload = serde_json::to_vec(&BacktestResultMsg::default()).unwrap();
    let reference = reference(&payload, payload.len() as u64);
    let mut download = ArtifactDownload::start(
        reference.clone(),
        directory.path(),
        ResultIoLimits::default(),
    )
    .unwrap();
    let error = download
        .accept(GetResultArtifactChunkResponse {
            success: true,
            artifact_id: reference.artifact_id,
            offset: 1,
            data_base64: STANDARD.encode(&payload),
            eof: true,
            error: None,
        })
        .unwrap_err();
    assert!(error.to_string().contains("offset mismatch"));
    assert_eq!(download.offset(), 0);
}

#[test]
fn malformed_chunks_are_rejected_without_advancing() {
    let directory = tempfile::tempdir().unwrap();
    let payload = serde_json::to_vec(&BacktestResultMsg::default()).unwrap();
    let reference = reference(&payload, payload.len() as u64);
    for response in [
        GetResultArtifactChunkResponse {
            success: true,
            artifact_id: "wrong".into(),
            offset: 0,
            data_base64: STANDARD.encode(&payload),
            eof: true,
            error: None,
        },
        GetResultArtifactChunkResponse {
            success: true,
            artifact_id: reference.artifact_id.clone(),
            offset: 0,
            data_base64: "***".into(),
            eof: true,
            error: None,
        },
        GetResultArtifactChunkResponse {
            success: true,
            artifact_id: reference.artifact_id.clone(),
            offset: 0,
            data_base64: String::new(),
            eof: false,
            error: None,
        },
    ] {
        let mut download = ArtifactDownload::start(
            reference.clone(),
            directory.path(),
            ResultIoLimits::default(),
        )
        .unwrap();
        assert!(download.accept(response).is_err());
        assert_eq!(download.offset(), 0);
    }
}

#[test]
fn missing_and_early_eof_are_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let payload = serde_json::to_vec(&BacktestResultMsg::default()).unwrap();
    let reference = reference(&payload, payload.len() as u64);
    let mut early = ArtifactDownload::start(
        reference.clone(),
        directory.path(),
        ResultIoLimits::default(),
    )
    .unwrap();
    assert!(
        early
            .accept(GetResultArtifactChunkResponse {
                success: true,
                artifact_id: reference.artifact_id.clone(),
                offset: 0,
                data_base64: STANDARD.encode(&payload[..payload.len() - 1]),
                eof: true,
                error: None,
            })
            .is_err()
    );
    assert_eq!(early.offset(), 0);
    let mut missing = ArtifactDownload::start(
        reference.clone(),
        directory.path(),
        ResultIoLimits::default(),
    )
    .unwrap();
    assert!(
        missing
            .accept(GetResultArtifactChunkResponse {
                success: true,
                artifact_id: reference.artifact_id,
                offset: 0,
                data_base64: STANDARD.encode(&payload),
                eof: false,
                error: None,
            })
            .is_err()
    );
    assert_eq!(missing.offset(), 0);
}

#[test]
fn digest_mismatch_is_rejected_after_complete_download() {
    let directory = tempfile::tempdir().unwrap();
    let payload = serde_json::to_vec(&BacktestResultMsg::default()).unwrap();
    let mut reference = reference(&payload, payload.len() as u64);
    reference.sha256 = "00".repeat(32);
    let mut download = ArtifactDownload::start(
        reference.clone(),
        directory.path(),
        ResultIoLimits::default(),
    )
    .unwrap();
    download
        .accept(GetResultArtifactChunkResponse {
            success: true,
            artifact_id: reference.artifact_id,
            offset: 0,
            data_base64: STANDARD.encode(&payload),
            eof: true,
            error: None,
        })
        .unwrap();
    assert!(download.finish(ResultIoLimits::default()).is_err());
}

fn reference(payload: &[u8], chunk_size: u64) -> ResultArtifactRefMsg {
    ResultArtifactRefMsg {
        format_version: RESULT_FORMAT_VERSION,
        artifact_id: "artifact-1".into(),
        byte_len: payload.len() as u64,
        sha256: format!("{:x}", Sha256::digest(payload)),
        chunk_size,
    }
}
