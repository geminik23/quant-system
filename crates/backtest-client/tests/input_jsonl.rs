use std::io::{self, Cursor, Read};
use std::sync::atomic::{AtomicU64, Ordering};

use qs_backtest_client::{
    BacktestInputInspector, InputWarning, InspectSignalInput, PreparationCancellation,
    SignalDecodingPolicy, SignalInputLimits, SignalInputSource, WorkflowError,
};

static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(1);

#[tokio::test]
async fn reader_display_name_redacts_absolute_paths() {
    let inspected = BacktestInputInspector
        .inspect(
            InspectSignalInput {
                signals: SignalInputSource::Reader {
                    display_name: "C:\\private\\signals.jsonl".into(),
                    reader: Box::new(Cursor::new(Vec::<u8>::new())),
                },
                source_coverage: None,
                decoding: SignalDecodingPolicy::Strict,
                limits: SignalInputLimits::default(),
                from: None,
                to: None,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(inspected.summary().display_name, "signals.jsonl");
}

#[tokio::test]
async fn inspection_preserves_order_lines_and_canonical_filter() {
    let bytes = format!(
        "\n{}\r\n{}\n",
        entry("2026-01-15T00:30:00+02:00"),
        close_all("2026-01-16T00:00:00Z")
    )
    .into_bytes();
    let inspected = inspect_reader(
        bytes.clone(),
        SignalInputLimits::default(),
        Some("2026-01-14T22:00:00Z"),
        Some("2026-01-15T00:00:00Z"),
    )
    .await
    .unwrap();

    let summary = inspected.summary();
    assert_eq!(summary.byte_len, bytes.len() as u64);

    assert_eq!(summary.physical_lines, 3);
    assert_eq!(summary.non_empty_lines, 2);
    assert_eq!(summary.signal_count, 2);
    assert_eq!(summary.retained_signal_count, 1);
    assert_eq!(summary.entry_count, 1);
    assert_eq!(summary.action_counts.get("Entry"), Some(&1));
    assert_eq!(
        summary.minimum_timestamp.as_deref(),
        Some("2026-01-14T22:30:00")
    );
    assert_eq!(
        summary.maximum_timestamp.as_deref(),
        Some("2026-01-14T22:30:00")
    );
    assert_eq!(inspected.signals()[0].ts(), "2026-01-14T22:30:00");
    assert_eq!(
        inspected.filter().from.as_deref(),
        Some("2026-01-14T22:00:00")
    );
    assert_eq!(
        inspected.filter().to.as_deref(),
        Some("2026-01-15T00:00:00")
    );
}

#[tokio::test]
async fn path_and_reader_use_the_same_scanner() {
    let bytes = format!(
        "{}\n\n{}",
        entry("2026-01-15T10:00:00"),
        close_all("2026-01-15T11:00:00")
    )
    .into_bytes();
    let path = std::env::temp_dir().join(format!(
        "qs-backtest-input-{}-{}.jsonl",
        std::process::id(),
        NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, &bytes).unwrap();

    let inspector = BacktestInputInspector;
    let from_path = inspector
        .inspect(
            InspectSignalInput {
                signals: SignalInputSource::Path(path.clone()),
                source_coverage: None,
                decoding: SignalDecodingPolicy::Strict,
                limits: SignalInputLimits::default(),
                from: None,
                to: None,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap();
    let from_reader = inspect_reader(bytes, SignalInputLimits::default(), None, None)
        .await
        .unwrap();
    std::fs::remove_file(path).unwrap();

    assert_eq!(
        from_path.summary().physical_lines,
        from_reader.summary().physical_lines
    );
    assert_eq!(
        from_path.summary().signal_count,
        from_reader.summary().signal_count
    );
}

#[tokio::test]
async fn byte_line_signal_and_utf8_limits_fail_with_safe_context() {
    let bytes = format!("{}\n", entry("2026-01-15T10:00:00")).into_bytes();
    let file_error = inspect_reader(
        bytes.clone(),
        SignalInputLimits {
            maximum_file_bytes: (bytes.len() - 1) as u64,
            ..SignalInputLimits::default()
        },
        None,
        None,
    )
    .await
    .unwrap_err();
    assert!(matches!(file_error, WorkflowError::InputByteLimit { .. }));

    let line_error = inspect_reader(
        bytes.clone(),
        SignalInputLimits {
            maximum_line_bytes: 8,
            ..SignalInputLimits::default()
        },
        None,
        None,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        line_error,
        WorkflowError::InputLineLimit { line: 1, .. }
    ));

    let two = format!(
        "{}\n{}\n",
        close_all("2026-01-15T10:00:00"),
        close_all("2026-01-15T11:00:00")
    )
    .into_bytes();
    let count_error = inspect_reader(
        two,
        SignalInputLimits {
            maximum_signal_count: 1,
            ..SignalInputLimits::default()
        },
        None,
        None,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        count_error,
        WorkflowError::SignalCountLimit { limit: 1, .. }
    ));

    let utf8_error = inspect_reader(
        vec![b'\n', 0xff, b'\n'],
        SignalInputLimits::default(),
        None,
        None,
    )
    .await
    .unwrap_err();
    assert!(matches!(
        utf8_error,
        WorkflowError::InvalidUtf8 { line: 2, .. }
    ));
}

#[tokio::test]
async fn exact_limits_inclusive_bounds_and_order_are_preserved() {
    let one = close_all("2026-01-15T10:00:00").into_bytes();
    let exact = inspect_reader(
        one.clone(),
        SignalInputLimits {
            maximum_file_bytes: one.len() as u64,
            maximum_line_bytes: one.len(),
            maximum_signal_count: 1,
            ..SignalInputLimits::default()
        },
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(exact.summary().signal_count, 1);

    let blank = inspect_reader(b"\n".to_vec(), SignalInputLimits::default(), None, None)
        .await
        .unwrap();
    assert_eq!(blank.summary().physical_lines, 1);
    assert_eq!(blank.summary().signal_count, 0);

    assert_eq!(blank.summary().warnings, vec![InputWarning::NoSignals]);

    let rows = format!(
        "{}\n{}\n{}",
        close_all("2026-01-15T10:00:00Z"),
        close_all("2026-01-15T10:30:00Z"),
        close_all("2026-01-15T11:00:00Z")
    )
    .into_bytes();
    let inclusive = inspect_reader(
        rows,
        SignalInputLimits::default(),
        Some("2026-01-15T10:00:00Z"),
        Some("2026-01-15T11:00:00Z"),
    )
    .await
    .unwrap();
    assert_eq!(
        inclusive
            .signals()
            .iter()
            .map(qs_backtest_api::RawSignalMsg::ts)
            .collect::<Vec<_>>(),
        vec![
            "2026-01-15T10:00:00",
            "2026-01-15T10:30:00",
            "2026-01-15T11:00:00",
        ]
    );
}

#[tokio::test]
async fn compatibility_preserves_nested_fields_but_not_invalid_entry_semantics() {
    for policy in [
        SignalDecodingPolicy::Strict,
        SignalDecodingPolicy::Compatibility,
    ] {
        let invalid = entry("2026-01-15T10:00:00")
            .replace("\"risk\":1.0", "\"risk\":0.0")
            .into_bytes();
        let error =
            inspect_reader_with_policy(invalid, SignalInputLimits::default(), None, None, policy)
                .await
                .unwrap_err();
        assert!(matches!(error, WorkflowError::SignalDecode { line: 1, .. }));
    }

    let obsolete = entry("2026-01-15T10:00:00")
        .replace("\"risk\":1.0", "\"size\":0.1")
        .into_bytes();
    let obsolete = inspect_reader(obsolete, SignalInputLimits::default(), None, None)
        .await
        .unwrap_err();
    assert!(matches!(
        obsolete,
        WorkflowError::SignalDecode { line: 1, .. }
    ));

    let scale_in = br#"{"action":"ScaleIn","ts":"2026-01-15T10:01:00","position":{"type":"ByTradeId","trade_id":"t1"},"price":null,"size":0.1}"#;
    let inspected = inspect_reader(scale_in.to_vec(), SignalInputLimits::default(), None, None)
        .await
        .unwrap();
    assert!(matches!(
        inspected.signals()[0],
        qs_backtest_api::RawSignalMsg::ScaleIn { size, .. } if size == 0.1
    ));
}

#[tokio::test]
async fn strict_and_compatibility_decoding_remain_distinct() {
    let row = br#"{"action":"Close","ts":"2026-01-15T10:00:00","position":{"type":"ByTradeId","trade_id":"t1","extra":true}}"#;
    let strict = inspect_reader(row.to_vec(), SignalInputLimits::default(), None, None)
        .await
        .unwrap_err();
    assert!(matches!(
        strict,
        WorkflowError::SignalDecode { line: 1, .. }
    ));

    let inspector = BacktestInputInspector;
    let compatible = inspector
        .inspect(
            InspectSignalInput {
                signals: SignalInputSource::Reader {
                    display_name: "compat.jsonl".into(),
                    reader: Box::new(Cursor::new(row.to_vec())),
                },
                source_coverage: None,
                decoding: SignalDecodingPolicy::Compatibility,
                limits: SignalInputLimits::default(),
                from: None,
                to: None,
            },
            PreparationCancellation::default(),
        )
        .await
        .unwrap();
    assert_eq!(compatible.summary().signal_count, 1);
    assert_eq!(
        compatible.summary().warnings,
        vec![InputWarning::NoEntrySignals]
    );
}

#[tokio::test]
async fn cancellation_is_observed_between_bounded_reads() {
    let cancellation = PreparationCancellation::default();
    let reader = CancelAfterFirstRead {
        bytes: Cursor::new(
            format!(
                "{}\n{}",
                entry("2026-01-15T10:00:00"),
                close_all("2026-01-15T11:00:00")
            )
            .into_bytes(),
        ),
        cancellation: cancellation.clone(),
        first: true,
    };
    let error = BacktestInputInspector
        .inspect(
            InspectSignalInput {
                signals: SignalInputSource::Reader {
                    display_name: "cancel.jsonl".into(),
                    reader: Box::new(reader),
                },
                source_coverage: None,
                decoding: SignalDecodingPolicy::Strict,
                limits: SignalInputLimits::default(),
                from: None,
                to: None,
            },
            cancellation,
        )
        .await
        .unwrap_err();
    assert_eq!(error, WorkflowError::PreparationCancelled);
}

#[tokio::test]
async fn cancelled_inspection_does_not_open_or_read_input() {
    let cancellation = PreparationCancellation::default();
    cancellation.cancel();
    let error = BacktestInputInspector
        .inspect(
            InspectSignalInput {
                signals: SignalInputSource::Path("does-not-exist.jsonl".into()),
                source_coverage: None,
                decoding: SignalDecodingPolicy::Strict,
                limits: SignalInputLimits::default(),
                from: None,
                to: None,
            },
            cancellation,
        )
        .await
        .unwrap_err();
    assert_eq!(error, WorkflowError::PreparationCancelled);
}

async fn inspect_reader(
    bytes: Vec<u8>,
    limits: SignalInputLimits,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<qs_backtest_client::InspectedSignalInput, WorkflowError> {
    inspect_reader_with_policy(bytes, limits, from, to, SignalDecodingPolicy::Strict).await
}

async fn inspect_reader_with_policy(
    bytes: Vec<u8>,
    limits: SignalInputLimits,
    from: Option<&str>,
    to: Option<&str>,
    decoding: SignalDecodingPolicy,
) -> Result<qs_backtest_client::InspectedSignalInput, WorkflowError> {
    BacktestInputInspector
        .inspect(
            InspectSignalInput {
                signals: SignalInputSource::Reader {
                    display_name: "signals.jsonl".into(),
                    reader: Box::new(Cursor::new(bytes)),
                },
                source_coverage: None,
                decoding,
                limits,
                from: from.map(str::to_owned),
                to: to.map(str::to_owned),
            },
            PreparationCancellation::default(),
        )
        .await
}

struct CancelAfterFirstRead {
    bytes: Cursor<Vec<u8>>,
    cancellation: PreparationCancellation,
    first: bool,
}

impl Read for CancelAfterFirstRead {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let maximum = buffer.len().min(64);
        let read = self.bytes.read(&mut buffer[..maximum])?;
        if self.first {
            self.first = false;
            self.cancellation.cancel();
        }
        Ok(read)
    }
}

fn entry(ts: &str) -> String {
    format!(
        r#"{{"action":"Entry","ts":"{ts}","symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,"risk":1.0,"stoploss":null,"targets":[],"group":null,"trade_id":"t1"}}"#
    )
}

fn close_all(ts: &str) -> String {
    format!(r#"{{"action":"CloseAll","ts":"{ts}"}}"#)
}
