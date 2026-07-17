use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

struct TempJsonl {
    path: PathBuf,
}

impl TempJsonl {
    fn new(label: &str, contents: &str) -> Self {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "tg_backtest_cli_{label}_{}_{}.jsonl",
            std::process::id(),
            unique
        ));
        std::fs::write(&path, contents).unwrap();
        Self { path }
    }

    fn as_str(&self) -> &str {
        self.path.to_str().unwrap()
    }
}

impl Drop for TempJsonl {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn run_tg_backtest(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_tg_backtest"))
        .args(args)
        .output()
        .expect("tg_backtest subprocess should start")
}

#[test]
fn empty_input_without_coverage_report_exits_nonzero() {
    let signals = TempJsonl::new("empty", "");
    let output = run_tg_backtest(&[
        "--input",
        signals.as_str(),
        "--exchange",
        "fixture",
        "--symbol",
        "EURUSD",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no signals were loaded"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("provider/coverage report"),
        "stderr: {stderr}"
    );
}

#[test]
fn all_filtered_input_without_coverage_report_exits_nonzero() {
    let signals = TempJsonl::new(
        "filtered",
        r#"{"action":"Entry","ts":"2026-01-15T10:00:00","symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,"risk":1.0,"stoploss":null,"targets":[],"group":null,"trade_id":null}
"#,
    );
    let output = run_tg_backtest(&[
        "--input",
        signals.as_str(),
        "--exchange",
        "fixture",
        "--symbol",
        "EURUSD",
        "--base-lot",
        "0.1",
        "--account-currency",
        "USD",
        "--from",
        "2027-01-01",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("date filters removed all 1 loaded signals"),
        "stderr: {stderr}"
    );
}

#[test]
fn invalid_timestamp_outcome_allows_zero_trade_coverage_report() {
    let signals = TempJsonl::new("coverage_empty", "");
    let outcomes = TempJsonl::new(
        "invalid_timestamp_outcome",
        r#"{"status":"failed","source":{"chat_id":42,"msg_id":7,"ts":"not-a-timestamp","message":"bad timestamp","reply_to":null},"parser":null,"failure":{"kind":"invalid_timestamp","value":"not-a-timestamp","reason":"unsupported source timestamp"}}
"#,
    );
    let output = run_tg_backtest(&[
        "--input",
        signals.as_str(),
        "--outcomes-input",
        outcomes.as_str(),
        "--exchange",
        "fixture",
        "--symbol",
        "EURUSD",
        "--account-currency",
        "USD",
        "--report",
        "provider",
    ]);

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "stdout: {stdout}\nstderr: {stderr}"
    );
    assert!(stdout.contains("raw=1, parsed=0, skipped=0, failed=1"));
    assert!(
        stdout.contains("\"provided_positions\": 0"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("\"selected_positions\": 0"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("\"failed_messages\": 1"),
        "stdout: {stdout}"
    );
    assert!(stdout.contains("\"raw_messages\": 1"), "stdout: {stdout}");
}

#[test]
fn entry_size_is_rejected_locally_with_its_jsonl_line_number() {
    let signals = TempJsonl::new(
        "obsolete_entry_size",
        r#"{"action":"CloseAll","ts":"2026-01-15T09:59:00"}
{"action":"Entry","ts":"2026-01-15T10:00:00","symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,"size":0.1,"stoploss":null,"targets":[],"group":null,"trade_id":null}
"#,
    );
    let output = run_tg_backtest(&[
        "--input",
        signals.as_str(),
        "--exchange",
        "fixture",
        "--symbol",
        "EURUSD",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("line 2"), "stderr: {stderr}");
    assert!(
        stderr.contains("Entry field `size` is obsolete"),
        "stderr: {stderr}"
    );
}

#[test]
fn entry_without_risk_is_rejected_locally_with_its_jsonl_line_number() {
    let signals = TempJsonl::new(
        "missing_entry_risk",
        r#"{"action":"Entry","ts":"2026-01-15T10:00:00","symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,"stoploss":null,"targets":[],"group":null,"trade_id":null}
"#,
    );
    let output = run_tg_backtest(&[
        "--input",
        signals.as_str(),
        "--exchange",
        "fixture",
        "--symbol",
        "EURUSD",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("line 1: Entry requires field `risk`"),
        "stderr: {stderr}"
    );
}

#[test]
fn entry_requires_sizing_and_account_currency_before_connecting() {
    let signals = TempJsonl::new(
        "entry_contract",
        r#"{"action":"Entry","ts":"2026-01-15T10:00:00","symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,"risk":1.0,"stoploss":null,"targets":[],"group":null,"trade_id":null}
"#,
    );

    let no_sizing = run_tg_backtest(&[
        "--input",
        signals.as_str(),
        "--exchange",
        "fixture",
        "--symbol",
        "EURUSD",
        "--account-currency",
        "USD",
    ]);
    let no_sizing_stderr = String::from_utf8_lossy(&no_sizing.stderr);
    assert!(!no_sizing.status.success());
    assert!(
        no_sizing_stderr.contains("Entry signals require exactly one of --base-lot"),
        "stderr: {no_sizing_stderr}"
    );

    let no_currency = run_tg_backtest(&[
        "--input",
        signals.as_str(),
        "--exchange",
        "fixture",
        "--symbol",
        "EURUSD",
        "--risk-percent",
        "1",
    ]);
    let no_currency_stderr = String::from_utf8_lossy(&no_currency.stderr);
    assert!(!no_currency.status.success());
    assert!(
        no_currency_stderr.contains("Entry signals require --account-currency"),
        "stderr: {no_currency_stderr}"
    );
}

#[test]
fn sizing_options_are_mutually_exclusive_in_command_line_parsing() {
    let output = run_tg_backtest(&[
        "--input",
        "signals.jsonl",
        "--exchange",
        "fixture",
        "--symbol",
        "EURUSD",
        "--base-lot",
        "0.1",
        "--risk-per-trade",
        "100",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot be used with"), "stderr: {stderr}");
    assert!(stderr.contains("--base-lot"), "stderr: {stderr}");
    assert!(stderr.contains("--risk-per-trade"), "stderr: {stderr}");
}

#[test]
fn scale_in_keeps_its_size_field() {
    let signals = TempJsonl::new(
        "scale_in_size",
        r#"{"action":"ScaleIn","ts":"2026-01-15T10:00:00","position":{"type":"ByTradeId","trade_id":"trade-1"},"price":null,"size":0.1}
"#,
    );
    let output = run_tg_backtest(&[
        "--input",
        signals.as_str(),
        "--exchange",
        "fixture",
        "--symbol",
        "EURUSD",
        "--from",
        "2027-01-01",
    ]);

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("date filters removed all 1 loaded signals"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("failed to parse raw signal"),
        "stderr: {stderr}"
    );
}

#[test]
fn help_documents_current_sizing_currency_and_future_defaults() {
    let output = run_tg_backtest(&["--help"]);
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);

    for option in [
        "--base-lot",
        "--risk-per-trade",
        "--risk-percent",
        "--account-currency",
        "--conversion-stale-after-ms",
        "--mtm-output",
        "--mtm-max-points",
        "--result-delivery",
    ] {
        assert!(stdout.contains(option), "help omitted {option}: {stdout}");
    }
    assert!(
        !stdout.contains("--execution-convention"),
        "stdout: {stdout}"
    );
    assert!(stdout.contains("[default: standard]"), "stdout: {stdout}");
    assert!(stdout.contains("[default: bounded]"), "stdout: {stdout}");
    assert!(stdout.contains("Default: 4096"), "stdout: {stdout}");
    assert!(
        stdout.contains("--risk-per-trade 100 --account-currency USD"),
        "stdout: {stdout}"
    );
}

#[test]
fn invalid_mtm_output_combinations_fail_before_loading_input() {
    let unrelated_max = run_tg_backtest(&[
        "--input",
        "missing-signals.jsonl",
        "--exchange",
        "fixture",
        "--mtm-output",
        "full",
        "--mtm-max-points",
        "64",
    ]);
    let unrelated_max_stderr = String::from_utf8_lossy(&unrelated_max.stderr);
    assert!(!unrelated_max.status.success());
    assert!(
        unrelated_max_stderr.contains("--mtm-max-points requires --mtm-output bounded"),
        "stderr: {unrelated_max_stderr}"
    );

    let out_of_range = run_tg_backtest(&[
        "--input",
        "missing-signals.jsonl",
        "--exchange",
        "fixture",
        "--mtm-max-points",
        "7",
    ]);
    let out_of_range_stderr = String::from_utf8_lossy(&out_of_range.stderr);
    assert!(!out_of_range.status.success());
    assert!(
        out_of_range_stderr.contains("--mtm-max-points must be between 8 and 16384"),
        "stderr: {out_of_range_stderr}"
    );

    let full_inline = run_tg_backtest(&[
        "--input",
        "missing-signals.jsonl",
        "--exchange",
        "fixture",
        "--mtm-output",
        "full",
        "--result-delivery",
        "inline",
    ]);
    let full_inline_stderr = String::from_utf8_lossy(&full_inline.stderr);
    assert!(!full_inline.status.success());
    assert!(
        full_inline_stderr
            .contains("--mtm-output full cannot be used with --result-delivery inline"),
        "stderr: {full_inline_stderr}"
    );
    assert!(
        !full_inline_stderr.contains("No such file"),
        "validation should run before loading input: {full_inline_stderr}"
    );
}
