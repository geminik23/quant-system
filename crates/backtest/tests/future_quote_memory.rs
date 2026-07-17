use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use qs_backtest::runner::BacktestConfig;
use qs_backtest::sizing::SizingPolicy;
use qs_backtest::{
    BacktestRunner, FutureQuoteConfig, MarketEvent, MtmOutputPolicy, RawSignal, VecFeed,
};
use qs_core::types::{OrderType, Side};
use qs_symbols::SymbolSpec;

const SYMBOL: &str = "ACTIVE";
const N: usize = 1_024;
const FIXED_ALLOWANCE_BYTES: usize = 1_048_576;

struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
static ALLOCATION_TEST_LOCK: Mutex<()> = Mutex::new(());

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            add_live_bytes(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::SeqCst);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let resized = unsafe { System.realloc(pointer, layout, new_size) };
        if !resized.is_null() {
            if new_size >= layout.size() {
                add_live_bytes(new_size - layout.size());
            } else {
                LIVE_BYTES.fetch_sub(layout.size() - new_size, Ordering::SeqCst);
            }
        }
        resized
    }
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

fn add_live_bytes(bytes: usize) {
    let live = LIVE_BYTES.fetch_add(bytes, Ordering::SeqCst) + bytes;
    let mut peak = PEAK_BYTES.load(Ordering::SeqCst);
    while live > peak {
        match PEAK_BYTES.compare_exchange_weak(peak, live, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(current) => peak = current,
        }
    }
}

fn ts(milliseconds: usize) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        + Duration::milliseconds(i64::try_from(milliseconds).unwrap())
}

fn fixture(primary_events: usize) -> (BacktestRunner, VecFeed, Vec<RawSignal>) {
    let events = (0..primary_events)
        .map(|index| MarketEvent::Tick {
            symbol: SYMBOL.to_owned(),
            ts: ts(index),
            bid: 100.0 + index as f64 * 0.01,
            ask: 100.0 + index as f64 * 0.01,
        })
        .collect();
    let config = BacktestConfig {
        close_on_finish: false,
        sizing: Some(SizingPolicy::FixedLot { lots: 1.0 }),
        symbol_specs: HashMap::from([(
            SYMBOL.to_owned(),
            SymbolSpec {
                canonical: SYMBOL.to_ascii_lowercase(),
                pip_position: 2,
                digits: 2,
                category: "index".to_owned(),
                lot_base_units: 1,
                lot_step_units: 1,
                lot_min_steps: 1,
                lot_max_steps: 0,
            },
        )]),
        ..BacktestConfig::default()
    };
    let future = FutureQuoteConfig {
        mtm_output: MtmOutputPolicy::Bounded { max_points: 32 },
        ..FutureQuoteConfig::default()
    };
    let signals = vec![RawSignal::Entry {
        ts: ts(0),
        symbol: SYMBOL.to_owned(),
        side: Side::Buy,
        order_type: OrderType::Market,
        price: None,
        risk_multiplier: 1.0,
        stoploss: Some(90.0),
        targets: Vec::new(),
        group: None,
        trade_id: Some("memory-slope".to_owned()),
    }];

    (
        BacktestRunner::new_future(config, future),
        VecFeed::new(events),
        signals,
    )
}

fn replay_peak_bytes(primary_events: usize) -> usize {
    let (runner, mut feed, signals) = fixture(primary_events);
    let baseline = LIVE_BYTES.load(Ordering::SeqCst);
    PEAK_BYTES.store(baseline, Ordering::SeqCst);

    let result = runner.run_raw_signals_future(&mut feed, signals, None);
    let peak = PEAK_BYTES.load(Ordering::SeqCst).saturating_sub(baseline);

    assert_eq!(feed.remaining(), 0);
    assert_eq!(result.open_position_snapshots.len(), 1);
    assert_eq!(
        result.open_position_snapshots[0].quote_ts,
        Some(ts(primary_events - 1))
    );
    assert_eq!(
        result.mtm_output_summary.observed_points,
        u64::try_from(primary_events + 2).unwrap()
    );
    assert_eq!(result.mtm_output_summary.retained_points, 32);
    std::hint::black_box(result);
    peak
}

#[test]
fn bounded_active_replay_peak_allocation_has_linear_slope() {
    let _guard = ALLOCATION_TEST_LOCK.lock().unwrap();
    let n_bytes = replay_peak_bytes(N);
    let two_n_bytes = replay_peak_bytes(2 * N);
    let limit = n_bytes.saturating_mul(5) / 2 + FIXED_ALLOWANCE_BYTES;

    eprintln!(
        "FutureQuote peak bytes: N={N} bytes={n_bytes}, 2N={} bytes={two_n_bytes}, limit={limit}",
        2 * N
    );
    assert!(
        two_n_bytes <= limit,
        "2N peak allocation {two_n_bytes} exceeded 2.5x N peak {n_bytes} plus {FIXED_ALLOWANCE_BYTES} bytes"
    );
}
