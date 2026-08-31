use std::fs;

use qs_backtest_api::BacktestResultMsg;
use qs_backtest_client::{
    OpenedResultFile, OutputCommit, OutputConflictPolicy, OutputTarget, ResultFileFormat,
    ResultIoLimits, ResultOutput, open_result_path, stage_output,
};

#[test]
fn no_clobber_commit_preserves_existing_target() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("existing result.json");
    fs::write(&path, b"owned").unwrap();
    let staged = stage_output(
        OutputTarget {
            path: path.clone(),
            format: ResultFileFormat::LegacyBareResult,
            conflict: OutputConflictPolicy::FailIfExists,
        },
        ResultOutput::Legacy(&BacktestResultMsg::default()),
        ResultIoLimits::default(),
    )
    .unwrap();
    assert!(matches!(
        staged.commit(ResultIoLimits::default()).unwrap(),
        OutputCommit::Conflict(_)
    ));
    assert_eq!(fs::read(path).unwrap(), b"owned");
}

#[test]
fn racing_writers_allow_exactly_one_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("race.json");
    let target = || OutputTarget {
        path: path.clone(),
        format: ResultFileFormat::LegacyBareResult,
        conflict: OutputConflictPolicy::FailIfExists,
    };
    let first = stage_output(
        target(),
        ResultOutput::Legacy(&BacktestResultMsg::default()),
        ResultIoLimits::default(),
    )
    .unwrap();
    let second = stage_output(
        target(),
        ResultOutput::Legacy(&BacktestResultMsg::default()),
        ResultIoLimits::default(),
    )
    .unwrap();
    let one = std::thread::spawn(move || first.commit(ResultIoLimits::default()));
    let two = std::thread::spawn(move || second.commit(ResultIoLimits::default()));
    let outcomes = [one.join().unwrap().unwrap(), two.join().unwrap().unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, OutputCommit::Committed(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, OutputCommit::Conflict(_)))
            .count(),
        1
    );
    assert!(matches!(
        open_result_path(&path, ResultIoLimits::default()).unwrap(),
        OpenedResultFile::Legacy(_)
    ));
}

#[test]
fn retarget_restages_in_the_new_target_directory() {
    let first_directory = tempfile::tempdir().unwrap();
    let second_directory = tempfile::tempdir().unwrap();
    let first_path = first_directory.path().join("occupied.json");
    let second_path = second_directory.path().join("saved.json");
    fs::write(&first_path, b"owned").unwrap();
    let staged = stage_output(
        OutputTarget {
            path: first_path,
            format: ResultFileFormat::LegacyBareResult,
            conflict: OutputConflictPolicy::FailIfExists,
        },
        ResultOutput::Legacy(&BacktestResultMsg::default()),
        ResultIoLimits::default(),
    )
    .unwrap();
    let OutputCommit::Conflict(staged) = staged.commit(ResultIoLimits::default()).unwrap() else {
        panic!("expected conflict");
    };
    let staged = staged
        .retarget(
            OutputTarget {
                path: second_path.clone(),
                format: ResultFileFormat::LegacyBareResult,
                conflict: OutputConflictPolicy::FailIfExists,
            },
            ResultIoLimits::default(),
        )
        .unwrap();
    assert_eq!(
        staged.temporary_path().parent(),
        Some(second_directory.path())
    );
    assert!(matches!(
        staged.commit(ResultIoLimits::default()).unwrap(),
        OutputCommit::Committed(_)
    ));
    assert!(second_path.is_file());
}

#[cfg(unix)]
#[test]
fn symlink_targets_are_not_replaced() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().unwrap();
    let owned = directory.path().join("owned.json");
    let regular_link = directory.path().join("regular-link.json");
    let dangling_link = directory.path().join("dangling-link.json");
    fs::write(&owned, b"owned").unwrap();
    symlink(&owned, &regular_link).unwrap();
    symlink(directory.path().join("missing.json"), &dangling_link).unwrap();
    for path in [regular_link, dangling_link] {
        let staged = stage_output(
            OutputTarget {
                path: path.clone(),
                format: ResultFileFormat::LegacyBareResult,
                conflict: OutputConflictPolicy::FailIfExists,
            },
            ResultOutput::Legacy(&BacktestResultMsg::default()),
            ResultIoLimits::default(),
        )
        .unwrap();
        assert!(matches!(
            staged.commit(ResultIoLimits::default()).unwrap(),
            OutputCommit::Conflict(_)
        ));
        assert!(fs::symlink_metadata(path).unwrap().file_type().is_symlink());
    }
    assert_eq!(fs::read(owned).unwrap(), b"owned");
}

#[test]
fn directory_target_is_not_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("target.json");
    fs::create_dir(&path).unwrap();
    let staged = stage_output(
        OutputTarget {
            path: path.clone(),
            format: ResultFileFormat::LegacyBareResult,
            conflict: OutputConflictPolicy::FailIfExists,
        },
        ResultOutput::Legacy(&BacktestResultMsg::default()),
        ResultIoLimits::default(),
    )
    .unwrap();
    let outcome = staged.commit(ResultIoLimits::default()).unwrap();
    assert!(matches!(outcome, OutputCommit::Conflict(_)));
    assert!(path.is_dir());
}
