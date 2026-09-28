//! Multi-instance configured replay over one account, with optional portfolio supervision.

use qs_risk::{
    ExposureFact, ExposureIntent, HaltCommand, IntentKind, PortfolioFacts, PortfolioSupervisor,
    Verdict,
};

use super::*;
use crate::artifacts::RecordedFill;
use crate::strategy::{
    ConfiguredInstance, DirectPortfolioInstance, INSTANCE_POSITION_TAG, MAX_PORTFOLIO_INSTANCES,
    MixedPortfolioBacktestResult, MixedPortfolioReplayError, PortfolioBacktestResult,
    PortfolioInstanceOutput, PortfolioReplayError, SupervisorEvent, SupervisorHaltAction,
    SupervisorOutput,
};

/// Supervision and attribution state of one portfolio replay.
pub(super) struct PortfolioReplayState {
    instance_ids: Vec<String>,
    supervisor: Option<PortfolioSupervisor>,
    /// Approved Entries by action ID with the risk they requested, which counts until the Entry opens, and while it rests as a pending order.
    reservations: BTreeMap<String, ExposureFact>,
    /// Action IDs that already have a terminal disposition, so their reservation no longer counts as not yet placed.
    disposed: BTreeSet<String>,
    seen_dispositions: usize,
    /// Portfolio instance of every action a strategy generated.
    action_instances: BTreeMap<String, usize>,
    events: Vec<SupervisorEvent>,
    halt_actions: Vec<SupervisorHaltAction>,
    unmarked_boundaries: u64,
    next_halt_action: u64,
}

impl PortfolioReplayState {
    fn new(instance_ids: Vec<String>, supervisor: Option<PortfolioSupervisor>) -> Self {
        Self {
            instance_ids,
            supervisor,
            reservations: BTreeMap::new(),
            disposed: BTreeSet::new(),
            seen_dispositions: 0,
            action_instances: BTreeMap::new(),
            events: Vec::new(),
            halt_actions: Vec::new(),
            unmarked_boundaries: 0,
            next_halt_action: 0,
        }
    }

    pub(super) fn begin_batch(&mut self, ts: NaiveDateTime, balance: f64) {
        if let Some(supervisor) = self.supervisor.as_mut() {
            supervisor.begin(ts, balance);
        }
    }

    /// The `instance` tag of every position a portfolio instance opened, keyed by position ID.
    pub(super) fn position_tags(
        &self,
        fills: &[RecordedFill],
    ) -> BTreeMap<String, BTreeMap<String, String>> {
        let mut tags = BTreeMap::new();
        for fill in fills {
            let Some(instance) = fill
                .action_id
                .as_ref()
                .and_then(|action_id| self.action_instances.get(action_id))
            else {
                continue;
            };
            tags.entry(fill.position_id.clone()).or_insert_with(|| {
                BTreeMap::from([(
                    INSTANCE_POSITION_TAG.to_owned(),
                    self.instance_ids[*instance].clone(),
                )])
            });
        }
        tags
    }

    fn into_output(self) -> Option<SupervisorOutput> {
        let supervisor = self.supervisor?;
        Some(SupervisorOutput {
            events: self.events,
            halt_actions: self.halt_actions,
            halts: supervisor.finish(),
            unmarked_boundaries: self.unmarked_boundaries,
        })
    }
}

/// Replay hook that drives one configured driver per instance and routes each instance only its own series inputs.
struct PortfolioReplayHook<'a> {
    drivers: Vec<ConfiguredStrategyReplayDriver<'a>>,
    /// Durations each instance's series declare per symbol.
    declared: Vec<BTreeMap<String, BTreeSet<u64>>>,
    /// First instant each instance reads.
    feed_from: Vec<Option<NaiveDateTime>>,
    state: PortfolioReplayState,
    failed: Option<usize>,
    /// Kinds of primary input seen so far, as ticks and bars.
    seen_input: (bool, bool),
    /// First timestamp at which the feed mixed ticks and stored bars.
    mixed_input: Option<NaiveDateTime>,
}

impl PortfolioReplayHook<'_> {
    fn reads(&self, instance: usize, event: &FeedEvent) -> bool {
        if self.feed_from[instance].is_some_and(|from| event.event.ts() < from) {
            return false;
        }
        let Some(durations) = self.declared[instance].get(event.event.symbol()) else {
            return false;
        };
        match &event.event {
            MarketEvent::Tick { .. } => true,
            MarketEvent::Bar {
                timeframe_seconds, ..
            } => timeframe_seconds.is_none_or(|seconds| durations.contains(&seconds)),
        }
    }
}

impl FutureReplayHook for PortfolioReplayHook<'_> {
    fn is_active(&self) -> bool {
        true
    }

    fn output_ready(&self) -> bool {
        self.drivers.iter().any(FutureReplayHook::output_ready)
    }

    /// A portfolio's primary feed is all ticks or all stored bars: a bar batch lets every instance decide before the batch's quotes settle, so a tick instance sharing a feed with bars would change its fill rule wherever the two happened to align.
    fn preflight_primary_events(&mut self, events: &[FeedEvent]) -> bool {
        for event in events {
            match event.event {
                MarketEvent::Tick { .. } => self.seen_input.0 = true,
                MarketEvent::Bar { .. } => self.seen_input.1 = true,
            }
            if self.seen_input == (true, true) {
                self.mixed_input = Some(event.event.ts());
                return false;
            }
        }
        for (index, driver) in self.drivers.iter_mut().enumerate() {
            if !driver.preflight_primary_events(events) {
                self.failed = Some(index);
                return false;
            }
        }
        true
    }

    fn reject_generated_configuration(&mut self, instance: Option<usize>, reason: String) {
        let index = instance.unwrap_or(0);
        self.failed = Some(index);
        self.drivers[index].reject_generated_configuration(None, reason);
    }

    fn observes_position_economics(&self) -> bool {
        true
    }

    fn reads_completed_bars_only(&self) -> bool {
        true
    }

    fn bar_execution_timeframes(&self) -> BTreeMap<String, u64> {
        let mut shortest = BTreeMap::<String, u64>::new();
        for driver in &self.drivers {
            for (symbol, seconds) in driver.bar_execution_timeframes() {
                shortest
                    .entry(symbol)
                    .and_modify(|current| *current = (*current).min(seconds))
                    .or_insert(seconds);
            }
        }
        shortest
    }

    fn portfolio_state(&mut self) -> Option<&mut PortfolioReplayState> {
        Some(&mut self.state)
    }

    fn on_boundary(
        &mut self,
        batch: &TimestampBatch,
        engine: &TradeEngine,
        lifecycle: &LifecycleLedger,
        positions: &BoundaryPositionFacts<'_>,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> Option<Vec<ScheduledSignal>> {
        let mut scheduled = Vec::new();
        for index in 0..self.drivers.len() {
            let own_batch = TimestampBatch {
                ts: batch.ts,
                events: batch
                    .events
                    .iter()
                    .filter(|event| self.reads(index, event))
                    .cloned()
                    .collect(),
            };
            // Every driver consumes and clears the feedback it is given, and ignores feedback for commands it did not issue.
            let mut effects = pending_effects.clone();
            let mut events = pending_events.clone();
            let Some(output) = self.drivers[index].on_boundary(
                &own_batch,
                engine,
                lifecycle,
                positions,
                &mut effects,
                &mut events,
            ) else {
                self.failed = Some(index);
                return None;
            };
            scheduled.extend(output.into_iter().map(|mut signal| {
                signal.instance = Some(index);
                signal
            }));
        }
        pending_effects.clear();
        pending_events.clear();
        Some(scheduled)
    }

    fn on_final_committed(
        &mut self,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> bool {
        for index in 0..self.drivers.len() {
            let mut effects = pending_effects.clone();
            let mut events = pending_events.clone();
            if !self.drivers[index].on_final_committed(&mut effects, &mut events) {
                self.failed = Some(index);
                return false;
            }
        }
        pending_effects.clear();
        pending_events.clear();
        true
    }
}

enum MixedDriverFailure {
    Configured(usize),
    Direct(usize),
}

struct MixedPortfolioReplayHook<'a, E> {
    configured: Vec<ConfiguredStrategyReplayDriver<'a>>,
    direct: Vec<StrategyReplayDriver<'a, dyn HistoricalStrategy<Error = E> + Send>>,
    configured_declared: Vec<BTreeMap<String, BTreeSet<u64>>>,
    direct_declared: Vec<BTreeMap<String, BTreeSet<u64>>>,
    configured_feed_from: Vec<Option<NaiveDateTime>>,
    direct_feed_from: Vec<Option<NaiveDateTime>>,
    state: PortfolioReplayState,
    failed: Option<MixedDriverFailure>,
    seen_input: (bool, bool),
    mixed_input: Option<NaiveDateTime>,
}

impl<E> MixedPortfolioReplayHook<'_, E> {
    fn reads(
        declared: &[BTreeMap<String, BTreeSet<u64>>],
        feed_from: &[Option<NaiveDateTime>],
        instance: usize,
        event: &FeedEvent,
    ) -> bool {
        if feed_from[instance].is_some_and(|from| event.available_at() < from) {
            return false;
        }
        let Some(durations) = declared[instance].get(event.event.symbol()) else {
            return false;
        };
        match &event.event {
            MarketEvent::Tick { .. } => true,
            MarketEvent::Bar {
                timeframe_seconds, ..
            } => timeframe_seconds.is_none_or(|seconds| durations.contains(&seconds)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn drive_configured(
        &mut self,
        batch: &TimestampBatch,
        engine: &TradeEngine,
        lifecycle: &LifecycleLedger,
        positions: &BoundaryPositionFacts<'_>,
        pending_effects: &[FutureEffect],
        pending_events: &[StrategyFeedbackEvent],
        scheduled: &mut Vec<ScheduledSignal>,
    ) -> bool {
        for index in 0..self.configured.len() {
            let own_batch = TimestampBatch {
                ts: batch.ts,
                events: batch
                    .events
                    .iter()
                    .filter(|event| {
                        Self::reads(
                            &self.configured_declared,
                            &self.configured_feed_from,
                            index,
                            event,
                        )
                    })
                    .cloned()
                    .collect(),
            };
            let mut effects = pending_effects.to_vec();
            let mut events = pending_events.to_vec();
            let Some(output) = self.configured[index].on_boundary(
                &own_batch,
                engine,
                lifecycle,
                positions,
                &mut effects,
                &mut events,
            ) else {
                self.failed = Some(MixedDriverFailure::Configured(index));
                return false;
            };
            scheduled.extend(output.into_iter().map(|mut signal| {
                signal.instance = Some(index);
                signal
            }));
        }
        true
    }

    #[allow(clippy::too_many_arguments)]
    fn drive_direct(
        &mut self,
        batch: &TimestampBatch,
        engine: &TradeEngine,
        lifecycle: &LifecycleLedger,
        positions: &BoundaryPositionFacts<'_>,
        pending_effects: &[FutureEffect],
        pending_events: &[StrategyFeedbackEvent],
        scheduled: &mut Vec<ScheduledSignal>,
    ) -> bool {
        let offset = self.configured.len();
        for index in 0..self.direct.len() {
            let own_batch = TimestampBatch {
                ts: batch.ts,
                events: batch
                    .events
                    .iter()
                    .filter(|event| {
                        Self::reads(&self.direct_declared, &self.direct_feed_from, index, event)
                    })
                    .cloned()
                    .collect(),
            };
            let mut effects = pending_effects.to_vec();
            let mut events = pending_events.to_vec();
            let Some(output) = self.direct[index].on_boundary(
                &own_batch,
                engine,
                lifecycle,
                positions,
                &mut effects,
                &mut events,
            ) else {
                self.failed = Some(MixedDriverFailure::Direct(index));
                return false;
            };
            scheduled.extend(output.into_iter().map(|mut signal| {
                signal.instance = Some(offset + index);
                signal
            }));
        }
        true
    }
}

impl<E> FutureReplayHook for MixedPortfolioReplayHook<'_, E> {
    fn is_active(&self) -> bool {
        true
    }

    fn output_ready(&self) -> bool {
        self.configured.iter().any(FutureReplayHook::output_ready)
            || self.direct.iter().any(FutureReplayHook::output_ready)
    }

    fn preflight_primary_events(&mut self, events: &[FeedEvent]) -> bool {
        for event in events {
            match event.event {
                MarketEvent::Tick { .. } => self.seen_input.0 = true,
                MarketEvent::Bar { .. } => self.seen_input.1 = true,
            }
            if self.seen_input == (true, true) {
                self.mixed_input = Some(event.available_at());
                return false;
            }
        }
        for (index, driver) in self.configured.iter_mut().enumerate() {
            if !driver.preflight_primary_events(events) {
                self.failed = Some(MixedDriverFailure::Configured(index));
                return false;
            }
        }
        for (index, driver) in self.direct.iter_mut().enumerate() {
            if !driver.preflight_primary_events(events) {
                self.failed = Some(MixedDriverFailure::Direct(index));
                return false;
            }
        }
        true
    }

    fn reject_generated_configuration(&mut self, instance: Option<usize>, reason: String) {
        let index = instance.unwrap_or(0);
        if index < self.configured.len() {
            self.failed = Some(MixedDriverFailure::Configured(index));
            self.configured[index].reject_generated_configuration(None, reason);
        } else {
            let direct = index - self.configured.len();
            self.failed = Some(MixedDriverFailure::Direct(direct));
            self.direct[direct].reject_generated_configuration(None, reason);
        }
    }

    fn observes_position_economics(&self) -> bool {
        true
    }

    fn reads_completed_bars_only(&self) -> bool {
        true
    }

    fn retains_post_bar_boundary(&self) -> bool {
        true
    }

    fn bar_execution_timeframes(&self) -> BTreeMap<String, u64> {
        let mut shortest = BTreeMap::new();
        for driver in &self.configured {
            for (symbol, seconds) in driver.bar_execution_timeframes() {
                shortest
                    .entry(symbol)
                    .and_modify(|current: &mut u64| *current = (*current).min(seconds))
                    .or_insert(seconds);
            }
        }
        for driver in &self.direct {
            for (symbol, seconds) in driver.bar_execution_timeframes() {
                shortest
                    .entry(symbol)
                    .and_modify(|current| *current = (*current).min(seconds))
                    .or_insert(seconds);
            }
        }
        shortest
    }

    fn portfolio_state(&mut self) -> Option<&mut PortfolioReplayState> {
        Some(&mut self.state)
    }

    fn on_pre_bar_boundary(
        &mut self,
        batch: &TimestampBatch,
        engine: &TradeEngine,
        lifecycle: &LifecycleLedger,
        positions: &BoundaryPositionFacts<'_>,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> Option<Vec<ScheduledSignal>> {
        let mut scheduled = Vec::new();
        self.drive_configured(
            batch,
            engine,
            lifecycle,
            positions,
            pending_effects,
            pending_events,
            &mut scheduled,
        )
        .then_some(scheduled)
    }

    fn on_boundary(
        &mut self,
        batch: &TimestampBatch,
        engine: &TradeEngine,
        lifecycle: &LifecycleLedger,
        positions: &BoundaryPositionFacts<'_>,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> Option<Vec<ScheduledSignal>> {
        let mut scheduled = Vec::new();
        let bar_batch = batch
            .events
            .iter()
            .any(|event| matches!(event.event, MarketEvent::Bar { .. }));
        if !bar_batch
            && !self.drive_configured(
                batch,
                engine,
                lifecycle,
                positions,
                pending_effects,
                pending_events,
                &mut scheduled,
            )
        {
            return None;
        }
        if !self.drive_direct(
            batch,
            engine,
            lifecycle,
            positions,
            pending_effects,
            pending_events,
            &mut scheduled,
        ) {
            return None;
        }
        pending_effects.clear();
        pending_events.clear();
        Some(scheduled)
    }

    fn on_final_committed(
        &mut self,
        pending_effects: &mut Vec<FutureEffect>,
        pending_events: &mut Vec<StrategyFeedbackEvent>,
    ) -> bool {
        for (index, driver) in self.configured.iter_mut().enumerate() {
            let mut effects = pending_effects.clone();
            let mut events = pending_events.clone();
            if !driver.on_final_committed(&mut effects, &mut events) {
                self.failed = Some(MixedDriverFailure::Configured(index));
                return false;
            }
        }
        for (index, driver) in self.direct.iter_mut().enumerate() {
            let mut effects = pending_effects.clone();
            let mut events = pending_events.clone();
            if !driver.on_final_committed(&mut effects, &mut events) {
                self.failed = Some(MixedDriverFailure::Direct(index));
                return false;
            }
        }
        pending_effects.clear();
        pending_events.clear();
        true
    }
}

impl BacktestRunner {
    /// Review the Entries and scale-ins a boundary generated, record a rejection for each refused one, and return what should be scheduled followed by any halt actions.
    ///
    /// When a halt begins at this boundary, every earlier approved Entry or scale-in that has not reached the market yet, whether waiting in `queued` for its symbol's next quote or in `scheduled` for its decision latency, is rejected as well, so a halt leaves no new exposure behind except what already filled or rests as a pending order, which its cancellation removes.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn supervise_generated<H: FutureReplayHook>(
        &mut self,
        hook: &mut H,
        batch_ts: NaiveDateTime,
        generated: Vec<ScheduledSignal>,
        decided_before_quotes: bool,
        drawdown_fraction: Option<f64>,
        scheduled: &mut VecDeque<ScheduledSignal>,
        queued: &mut VecDeque<QueuedAction>,
        lifecycle: &mut LifecycleLedger,
        future_executor: &FutureExecutor,
    ) -> Vec<ScheduledSignal> {
        let Some(state) = hook.portfolio_state() else {
            return generated;
        };
        for signal in &generated {
            if let (Some(instance), Some(action_id)) = (signal.instance, signal.action_id.as_ref())
            {
                state.action_instances.insert(action_id.clone(), instance);
            }
        }
        if state.supervisor.is_none() {
            return generated;
        }
        for disposition in &lifecycle.as_slice()[state.seen_dispositions..] {
            state.disposed.insert(disposition.action_id.clone());
        }
        state.seen_dispositions = lifecycle.len();
        if drawdown_fraction.is_none() {
            state.unmarked_boundaries += 1;
        }

        let open = self
            .engine
            .open_positions()
            .into_iter()
            .map(|position| ExposureFact {
                symbol: position.data.symbol.clone(),
                side: position.data.side,
                risk: future_executor.open_initial_risk(&position.data.id),
            })
            .collect::<Vec<_>>();
        let pending = self
            .engine
            .pending_positions()
            .into_iter()
            .map(|position| ExposureFact {
                symbol: position.data.symbol.clone(),
                side: position.data.side,
                risk: future_executor
                    .pending_metadata(&position.data.id)
                    .and_then(|(action_id, ..)| state.reservations.get(&action_id))
                    .and_then(|reserved| reserved.risk),
            })
            .collect::<Vec<_>>();
        let mut reserved = state
            .reservations
            .iter()
            .filter(|(action_id, _)| !state.disposed.contains(*action_id))
            .map(|(_, fact)| fact.clone())
            .collect::<Vec<_>>();
        let balance = future_executor.balance();
        let supervisor = state.supervisor.as_mut().expect("supervisor checked above");
        let day_realized_r = supervisor.day_start().map_or(0.0, |start| {
            future_executor
                .completed_positions
                .iter()
                .filter(|position| position.close_ts >= start)
                .filter_map(|position| position.realized_r)
                .sum()
        });
        let halt_commands = supervisor.on_boundary(&PortfolioFacts {
            now: batch_ts,
            balance,
            drawdown_fraction,
            day_realized_r,
            open: &open,
            pending: &pending,
            reserved: &reserved,
        });
        let halt_began = !halt_commands.is_empty();

        let mut approved = Vec::with_capacity(generated.len());
        let mut rejections = Vec::new();
        for signal in generated {
            let (symbol, side, kind, requested_risk) = match &signal.signal {
                RawSignal::Entry {
                    symbol,
                    side,
                    risk_multiplier,
                    ..
                } => (
                    symbol.clone(),
                    *side,
                    IntentKind::Entry,
                    requested_account_risk(self.config.sizing.as_ref(), balance, *risk_multiplier),
                ),
                RawSignal::ScaleIn { .. } => {
                    let Some((symbol, side)) = self
                        .resolve_future_actions(&signal.signal)
                        .into_iter()
                        .find_map(|action| match action {
                            Action::ScaleIn { position_id, .. } => self
                                .engine
                                .get_position(&position_id)
                                .map(|position| (position.data.symbol.clone(), position.data.side)),
                            _ => None,
                        })
                    else {
                        approved.push(signal);
                        continue;
                    };
                    (symbol, side, IntentKind::ScaleIn, None)
                }
                _ => {
                    approved.push(signal);
                    continue;
                }
            };
            let verdict = supervisor.review(
                &PortfolioFacts {
                    now: batch_ts,
                    balance,
                    drawdown_fraction,
                    day_realized_r,
                    open: &open,
                    pending: &pending,
                    reserved: &reserved,
                },
                &ExposureIntent {
                    symbol: &symbol,
                    side,
                    kind,
                    requested_risk,
                },
            );
            let action_id = signal.resolved_action_id();
            let instance_id = signal
                .instance
                .map(|instance| state.instance_ids[instance].clone())
                .unwrap_or_default();
            state.events.push(SupervisorEvent {
                ts: batch_ts,
                instance_id,
                action_id: action_id.clone(),
                kind,
                symbol: symbol.clone(),
                requested_risk,
                verdict: verdict.clone(),
            });
            match verdict {
                Verdict::Approve => {
                    if kind == IntentKind::Entry {
                        let fact = ExposureFact {
                            symbol,
                            side,
                            risk: requested_risk,
                        };
                        state.reservations.insert(action_id, fact.clone());
                        reserved.push(fact);
                    }
                    approved.push(signal);
                }
                Verdict::Reject { policy, reason } => {
                    let mut disposition =
                        ActionDisposition::rejected(action_id, format!("{policy}: {reason}"));
                    disposition.action_kind = Some(raw_signal_kind(&signal.signal).to_owned());
                    disposition.signal_ts = Some(signal.signal_ts);
                    disposition.effective_ts = Some(signal.effective_ts);
                    rejections.push(disposition);
                }
            }
        }
        for command in halt_commands {
            let (signal, name) = match command {
                HaltCommand::CancelAllPending => (
                    RawSignal::CancelAllPending { ts: batch_ts },
                    "cancel_all_pending",
                ),
                HaltCommand::CloseAll => (RawSignal::CloseAll { ts: batch_ts }, "close_all"),
            };
            let action_id = format!("supervisor:{:08}:{name}", state.next_halt_action);
            state.next_halt_action += 1;
            state.halt_actions.push(SupervisorHaltAction {
                ts: batch_ts,
                action_id: action_id.clone(),
                command,
            });
            approved.push(
                ScheduledSignal::new(0, batch_ts, batch_ts, signal, !decided_before_quotes)
                    .with_action_base(action_id),
            );
        }
        if halt_began {
            let policy = supervisor
                .intervals()
                .last()
                .map_or_else(|| "halt".to_owned(), |interval| interval.policy.clone());
            let reason = format!(
                "{policy}: new exposure is halted and the request had not reached the market"
            );
            let mut remaining = VecDeque::with_capacity(queued.len());
            for action in queued.drain(..) {
                if action.entry_signal.is_some()
                    || matches!(action.action, Action::Open { .. } | Action::ScaleIn { .. })
                {
                    let mut disposition =
                        ActionDisposition::rejected(action.action_id, reason.clone());
                    disposition.action_kind = Some(action.action_kind);
                    disposition.signal_ts = Some(action.signal_ts);
                    disposition.effective_ts = Some(action.effective_ts);
                    rejections.push(disposition);
                } else {
                    remaining.push_back(action);
                }
            }
            *queued = remaining;
            let mut remaining = VecDeque::with_capacity(scheduled.len());
            for signal in scheduled.drain(..) {
                if matches!(
                    signal.signal,
                    RawSignal::Entry { .. } | RawSignal::ScaleIn { .. }
                ) {
                    let mut disposition =
                        ActionDisposition::rejected(signal.resolved_action_id(), reason.clone());
                    disposition.action_kind = Some(raw_signal_kind(&signal.signal).to_owned());
                    disposition.signal_ts = Some(signal.signal_ts);
                    disposition.effective_ts = Some(signal.effective_ts);
                    rejections.push(disposition);
                } else {
                    remaining.push_back(signal);
                }
            }
            *scheduled = remaining;
        }
        for disposition in rejections {
            self.record_disposition(lifecycle, disposition);
        }
        approved
    }

    /// Run several configured strategy instances from a materialized data feed against one account.
    pub fn run_portfolio_future<F>(
        self,
        source_feed: &mut F,
        instances: Vec<ConfiguredInstance>,
        supervisor: Option<PortfolioSupervisor>,
        retention: StrategyRetentionLimits,
    ) -> Result<PortfolioBacktestResult, PortfolioReplayError<Infallible>>
    where
        F: DataFeed,
    {
        let mut ordered_events = Vec::new();
        let mut source_last_ts = BTreeMap::<String, NaiveDateTime>::new();
        while let Some(batch) = source_feed.next_batch() {
            for event in batch.events {
                let symbol = event.event.symbol().to_owned();
                let timestamp = event.event.ts();
                if source_last_ts
                    .get(&symbol)
                    .is_some_and(|previous| *previous > timestamp)
                {
                    continue;
                }
                source_last_ts.insert(symbol, timestamp);
                ordered_events.push(event);
            }
        }
        ordered_events.sort_by_key(FeedEvent::ordering_key);
        let primary_eod = ordered_events
            .iter()
            .filter(|event| event.metadata.roles.primary)
            .filter_map(|event| event.event.to_valid_quote())
            .map(|quote| quote.ts)
            .max();
        let mut ordered_feed = crate::data_feed::VecFeed::from_feed_events(ordered_events);
        let mut feed = DataFeedBatchAdapter {
            feed: &mut ordered_feed,
        };
        self.run_portfolio_future_streaming_controlled(
            &mut feed,
            primary_eod,
            instances,
            supervisor,
            retention,
            || false,
            |_| {},
        )
    }

    /// Run configured and caller-compiled strategies against one shared account while preserving their distinct bar timing.
    #[allow(clippy::too_many_arguments)]
    pub fn run_mixed_portfolio_future<F, E>(
        self,
        source_feed: &mut F,
        configured: Vec<ConfiguredInstance>,
        direct: Vec<DirectPortfolioInstance<E>>,
        supervisor: Option<PortfolioSupervisor>,
        retention: StrategyRetentionLimits,
    ) -> Result<MixedPortfolioBacktestResult, MixedPortfolioReplayError<Infallible>>
    where
        F: DataFeed,
        E: std::fmt::Display,
    {
        let mut events = Vec::new();
        while let Some(batch) = source_feed.next_batch() {
            events.extend(batch.events);
        }
        events.sort_by_key(FeedEvent::ordering_key);
        let primary_eod = events
            .iter()
            .filter(|event| event.metadata.roles.primary)
            .map(FeedEvent::available_at)
            .max();
        let mut feed = crate::data_feed::VecFeed::from_feed_events(events);
        let mut feed = DataFeedBatchAdapter { feed: &mut feed };
        self.run_mixed_portfolio_future_streaming_controlled(
            &mut feed,
            primary_eod,
            configured,
            direct,
            supervisor,
            retention,
            || false,
            |_| {},
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_mixed_portfolio_future_streaming_controlled<F, E, C, P>(
        mut self,
        feed: &mut F,
        primary_eod: Option<NaiveDateTime>,
        configured: Vec<ConfiguredInstance>,
        direct: Vec<DirectPortfolioInstance<E>>,
        supervisor: Option<PortfolioSupervisor>,
        retention: StrategyRetentionLimits,
        mut is_cancelled: C,
        mut on_progress: P,
    ) -> Result<MixedPortfolioBacktestResult, MixedPortfolioReplayError<F::Error>>
    where
        F: FallibleBatchFeed,
        E: std::fmt::Display,
        C: FnMut() -> bool,
        P: FnMut(ReplayProgress),
    {
        let total = configured.len().checked_add(direct.len()).ok_or_else(|| {
            MixedPortfolioReplayError::Input("mixed instance count overflowed".into())
        })?;
        if total == 0 {
            return Err(MixedPortfolioReplayError::NoInstances);
        }
        if total > MAX_PORTFOLIO_INSTANCES {
            return Err(MixedPortfolioReplayError::TooManyInstances(total));
        }
        if self.entry_profiles.is_some() {
            return Err(MixedPortfolioReplayError::Input(
                "mixed portfolio instances carry their own entry profiles".into(),
            ));
        }
        if self.config.run_tags.contains_key(INSTANCE_POSITION_TAG) {
            return Err(MixedPortfolioReplayError::Input(format!(
                "run tag '{INSTANCE_POSITION_TAG}' is owned by portfolio replay"
            )));
        }
        if let Some(supervisor) = supervisor.as_ref()
            && supervisor.caps_group_risk()
            && !self.config.sizing.as_ref().is_some_and(is_monetary_sizing)
        {
            return Err(MixedPortfolioReplayError::Input(
                "a group risk cap needs a monetary sizing policy".into(),
            ));
        }
        let future = self.future_config.clone().unwrap_or_default();
        self.future_config = Some(future.clone());
        validate_replay_config(&self.config, Some(&future), &[])
            .map_err(MixedPortfolioReplayError::Input)?;

        let mut identities = BTreeSet::new();
        let mut adapters = Vec::with_capacity(configured.len());
        let mut configured_analysis = Vec::with_capacity(configured.len());
        let mut configured_series = Vec::with_capacity(configured.len());
        let mut configured_feed_from = Vec::with_capacity(configured.len());
        let mut profiles = Vec::with_capacity(total);
        let mut instance_ids = Vec::with_capacity(total);
        for instance in configured {
            let instance_id = instance.instance_id().to_owned();
            if !identities.insert(instance_id.clone()) {
                return Err(MixedPortfolioReplayError::DuplicateInstanceIdentity { instance_id });
            }
            instance
                .adapter
                .preflight_entry_profiles(&instance.entry_profiles)
                .map_err(|error| MixedPortfolioReplayError::Instance {
                    instance_id: instance_id.clone(),
                    reason: error.to_string(),
                })?;
            instance
                .adapter
                .preflight(retention, self.strategy_research_limits)
                .map_err(|error| MixedPortfolioReplayError::Instance {
                    instance_id: instance_id.clone(),
                    reason: error.to_string(),
                })?;
            let specs = instance.adapter.series_specs().cloned().collect::<Vec<_>>();
            crate::strategy::replay::validate_series_specs(instance.adapter.requirements(), &specs)
                .map_err(|error| MixedPortfolioReplayError::Instance {
                    instance_id: instance_id.clone(),
                    reason: error.to_string(),
                })?;
            configured_series.push(MultiTimeframeSeries::new(specs).map_err(|error| {
                MixedPortfolioReplayError::Instance {
                    instance_id: instance_id.clone(),
                    reason: error.to_string(),
                }
            })?);
            instance_ids.push(instance_id);
            profiles.push(instance.entry_profiles);
            configured_feed_from.push(instance.feed_from);
            configured_analysis.push(instance.analysis);
            adapters.push(instance.adapter);
        }

        let mut direct_strategies = Vec::with_capacity(direct.len());
        let mut direct_analysis = Vec::with_capacity(direct.len());
        let mut direct_series = Vec::with_capacity(direct.len());
        let mut direct_feed_from = Vec::with_capacity(direct.len());
        for instance in direct {
            let instance_id = instance.instance_id;
            if !identities.insert(instance_id.clone()) {
                return Err(MixedPortfolioReplayError::DuplicateInstanceIdentity { instance_id });
            }
            crate::strategy::replay::validate_series_specs(
                instance.strategy.requirements(),
                &instance.series,
            )
            .map_err(|error| MixedPortfolioReplayError::Instance {
                instance_id: instance_id.clone(),
                reason: error.to_string(),
            })?;
            direct_series.push(MultiTimeframeSeries::new(instance.series).map_err(|error| {
                MixedPortfolioReplayError::Instance {
                    instance_id: instance_id.clone(),
                    reason: error.to_string(),
                }
            })?);
            instance_ids.push(instance_id);
            profiles.push(instance.entry_profiles);
            direct_feed_from.push(instance.feed_from);
            direct_analysis.push(instance.analysis);
            direct_strategies.push(instance.strategy);
        }
        self.instance_profiles = profiles;

        let configured_count = adapters.len();
        let research_limits = self.strategy_research_limits;
        let configured_drivers = adapters
            .iter_mut()
            .zip(configured_series)
            .zip(configured_analysis)
            .map(|((adapter, series), analysis)| {
                ConfiguredStrategyReplayDriver::new(
                    adapter,
                    series,
                    analysis,
                    retention,
                    research_limits,
                )
            })
            .collect::<Vec<_>>();
        let direct_drivers = direct_strategies
            .iter_mut()
            .zip(direct_series)
            .zip(direct_analysis)
            .map(|((strategy, series), analysis)| {
                StrategyReplayDriver::new(
                    strategy.as_mut(),
                    series,
                    analysis,
                    retention,
                    research_limits,
                )
            })
            .collect::<Vec<_>>();
        let configured_declared = configured_drivers
            .iter()
            .map(|driver| declared_series(&driver.requirements))
            .collect();
        let direct_declared = direct_drivers
            .iter()
            .map(|driver| declared_series(&driver.requirements))
            .collect();
        let mut hook = MixedPortfolioReplayHook {
            configured: configured_drivers,
            direct: direct_drivers,
            configured_declared,
            direct_declared,
            configured_feed_from,
            direct_feed_from,
            state: PortfolioReplayState::new(instance_ids.clone(), supervisor),
            failed: None,
            seen_input: (false, false),
            mixed_input: None,
        };
        let replay = match self.run_raw_signals_future_batches(
            feed,
            primary_eod,
            Vec::new(),
            None,
            future,
            None,
            0,
            0,
            &mut is_cancelled,
            &mut on_progress,
            &mut hook,
        ) {
            Ok(replay) => replay,
            Err(FutureBatchReplayError::Feed(error)) => {
                return Err(MixedPortfolioReplayError::Feed(error));
            }
            Err(FutureBatchReplayError::Cancelled) => {
                return Err(MixedPortfolioReplayError::Cancelled);
            }
            Err(FutureBatchReplayError::Dynamic) => {
                if let Some(timestamp) = hook.mixed_input {
                    return Err(MixedPortfolioReplayError::MixedPrimaryInput { timestamp });
                }
                let failure = hook
                    .failed
                    .take()
                    .unwrap_or(MixedDriverFailure::Configured(0));
                let (instance, reason) = match failure {
                    MixedDriverFailure::Configured(index) => {
                        let reason = hook
                            .configured
                            .swap_remove(index)
                            .finish()
                            .err()
                            .map_or_else(
                                || "configured instance failed".into(),
                                |error| format!("{error:?}"),
                            );
                        (index, reason)
                    }
                    MixedDriverFailure::Direct(index) => {
                        let reason = hook.direct.swap_remove(index).finish().err().map_or_else(
                            || "direct instance failed".into(),
                            |error| mixed_direct_error(&error),
                        );
                        (configured_count + index, reason)
                    }
                };
                return Err(MixedPortfolioReplayError::Instance {
                    instance_id: instance_ids[instance].clone(),
                    reason,
                });
            }
        };
        for (index, driver) in hook.configured.into_iter().enumerate() {
            driver
                .finish()
                .map_err(|error| MixedPortfolioReplayError::Instance {
                    instance_id: instance_ids[index].clone(),
                    reason: format!("{error:?}"),
                })?;
        }
        for (index, driver) in hook.direct.into_iter().enumerate() {
            driver
                .finish()
                .map_err(|error| MixedPortfolioReplayError::Instance {
                    instance_id: instance_ids[configured_count + index].clone(),
                    reason: mixed_direct_error(&error),
                })?;
        }
        Ok(MixedPortfolioBacktestResult {
            replay,
            supervisor: hook.state.into_output(),
        })
    }

    /// Run several configured strategy instances from complete ordered timestamp batches against one account, with cooperative cancellation and replay progress.
    ///
    /// Each instance keeps its own series, analysis, decisions, and entry profiles; fills, balance, costs, marks, and drawdown are shared. When a supervisor is supplied, every Entry and scale-in an instance generates is reviewed before it is scheduled, and a refused one reaches the instance as a rejected command.
    #[allow(clippy::too_many_arguments)]
    pub fn run_portfolio_future_streaming_controlled<F, C, P>(
        mut self,
        feed: &mut F,
        primary_eod: Option<NaiveDateTime>,
        instances: Vec<ConfiguredInstance>,
        supervisor: Option<PortfolioSupervisor>,
        retention: StrategyRetentionLimits,
        mut is_cancelled: C,
        mut on_progress: P,
    ) -> Result<PortfolioBacktestResult, PortfolioReplayError<F::Error>>
    where
        F: FallibleBatchFeed,
        C: FnMut() -> bool,
        P: FnMut(ReplayProgress),
    {
        if instances.is_empty() {
            return Err(PortfolioReplayError::NoInstances);
        }
        if instances.len() > MAX_PORTFOLIO_INSTANCES {
            return Err(PortfolioReplayError::TooManyInstances(instances.len()));
        }
        // The instance identifier alone labels positions, reviews, and outputs, so it must be unique across the portfolio, which also keeps every generated trade and command identifier unique.
        let mut identities = BTreeSet::new();
        for instance in &instances {
            if !identities.insert(instance.instance_id().to_owned()) {
                return Err(PortfolioReplayError::DuplicateInstanceIdentity {
                    instance_id: instance.instance_id().to_owned(),
                });
            }
        }
        if self.entry_profiles.is_some() {
            return Err(PortfolioReplayError::Input(
                StrategyReplayInputError::ManagementProfile(
                    "portfolio instances carry their own entry profiles, so the runner must not set run-level routes".into(),
                ),
            ));
        }
        if self.config.run_tags.contains_key(INSTANCE_POSITION_TAG) {
            return Err(PortfolioReplayError::Input(
                StrategyReplayInputError::FutureQuote(format!(
                    "run tag '{INSTANCE_POSITION_TAG}' is owned by the portfolio replay, which labels every position with its instance"
                )),
            ));
        }
        if let Some(supervisor) = supervisor.as_ref()
            && supervisor.caps_group_risk()
            && !self.config.sizing.as_ref().is_some_and(is_monetary_sizing)
        {
            return Err(PortfolioReplayError::Supervisor(
                "a group risk cap needs a monetary sizing policy, because a fixed-lot entry's risk is unknown until it fills".into(),
            ));
        }
        let future = self.future_config.clone().unwrap_or_default();
        self.future_config = Some(future.clone());
        validate_replay_config(&self.config, Some(&future), &[])
            .map_err(StrategyReplayInputError::FutureQuote)?;

        let mut adapters = Vec::with_capacity(instances.len());
        let mut analyses = Vec::with_capacity(instances.len());
        let mut instance_ids = Vec::with_capacity(instances.len());
        let mut strategy_ids = Vec::with_capacity(instances.len());
        let mut profiles = Vec::with_capacity(instances.len());
        let mut feed_from = Vec::with_capacity(instances.len());
        let mut series = Vec::with_capacity(instances.len());
        for instance in instances {
            let instance_id = instance.instance_id().to_owned();
            let fail = |source: StrategyReplayInputError| PortfolioReplayError::Instance {
                instance_id: instance_id.clone(),
                source: StrategyReplayError::Input(source),
            };
            instance
                .adapter
                .preflight_entry_profiles(&instance.entry_profiles)
                .map_err(|error| fail(error.into()))?;
            instance
                .adapter
                .preflight(retention, self.strategy_research_limits)
                .map_err(|error| fail(error.into()))?;
            // The account must be able to size and settle every symbol an instance can enter, which a single run would only discover at its first Entry.
            if !instance
                .adapter
                .configured_requirements()
                .entries
                .is_empty()
            {
                let symbol = instance.adapter.configured_strategy().primary_symbol();
                let problem = match self.config.sizing.as_ref() {
                    None => Some(
                        "a strategy that emits Entry actions requires a sizing policy".to_owned(),
                    ),
                    Some(_)
                        if !self.config.symbol_specs.contains_key(symbol)
                            && explicit_instrument_spec(&self.config, symbol).is_none() =>
                    {
                        Some(format!("missing instrument or symbol spec for {symbol}"))
                    }
                    Some(policy)
                        if is_monetary_sizing(policy)
                            && !future.currency_plan.as_ref().is_some_and(|plan| {
                                plan.route_for_primary_symbol(symbol).is_some()
                            }) =>
                    {
                        Some(format!(
                            "monetary sizing needs a currency plan route for primary symbol {symbol}"
                        ))
                    }
                    Some(_) => None,
                };
                if let Some(problem) = problem {
                    return Err(fail(StrategyReplayInputError::FutureQuote(problem)));
                }
            }
            let specs = instance.adapter.series_specs().cloned().collect::<Vec<_>>();
            crate::strategy::replay::validate_series_specs(instance.adapter.requirements(), &specs)
                .map_err(fail)?;
            let instance_series = MultiTimeframeSeries::new(specs).map_err(|error| {
                PortfolioReplayError::Instance {
                    instance_id: instance_id.clone(),
                    source: StrategyReplayError::Series(error),
                }
            })?;
            strategy_ids.push(instance.strategy_id().to_owned());
            instance_ids.push(instance_id);
            profiles.push(instance.entry_profiles);
            feed_from.push(instance.feed_from);
            analyses.push(instance.analysis);
            series.push(instance_series);
            adapters.push(instance.adapter);
        }
        self.instance_profiles = profiles.clone();

        let research_limits = self.strategy_research_limits;
        let descriptors = adapters
            .iter()
            .map(|adapter| adapter.descriptor().clone())
            .collect::<Vec<_>>();
        let drivers = adapters
            .iter_mut()
            .zip(series)
            .zip(analyses)
            .map(|((adapter, series), analysis)| {
                ConfiguredStrategyReplayDriver::new(
                    adapter,
                    series,
                    analysis,
                    retention,
                    research_limits,
                )
            })
            .collect::<Vec<_>>();
        let declared = drivers
            .iter()
            .map(|driver| {
                let mut declared = BTreeMap::<String, BTreeSet<u64>>::new();
                for requirement in driver.requirements.series() {
                    declared
                        .entry(requirement.symbol().to_owned())
                        .or_default()
                        .insert(requirement.timeframe().duration_seconds());
                }
                declared
            })
            .collect();
        let mut hook = PortfolioReplayHook {
            drivers,
            declared,
            feed_from,
            state: PortfolioReplayState::new(instance_ids.clone(), supervisor),
            failed: None,
            seen_input: (false, false),
            mixed_input: None,
        };
        let replay = match self.run_raw_signals_future_batches(
            feed,
            primary_eod,
            Vec::new(),
            None,
            future,
            None,
            0,
            0,
            &mut is_cancelled,
            &mut on_progress,
            &mut hook,
        ) {
            Ok(replay) => replay,
            Err(FutureBatchReplayError::Feed(error)) => {
                return Err(PortfolioReplayError::Feed(error));
            }
            Err(FutureBatchReplayError::Cancelled) => return Err(PortfolioReplayError::Cancelled),
            Err(FutureBatchReplayError::Dynamic) => {
                if let Some(timestamp) = hook.mixed_input {
                    return Err(PortfolioReplayError::MixedPrimaryInput { timestamp });
                }
                let index = hook.failed.unwrap_or(0);
                let driver = hook.drivers.swap_remove(index);
                let error = driver
                    .finish()
                    .expect_err("dynamic failure stores its cause");
                return Err(PortfolioReplayError::Instance {
                    instance_id: instance_ids[index].clone(),
                    source: map_strategy_driver_error(error),
                });
            }
        };
        let PortfolioReplayHook { drivers, state, .. } = hook;
        let mut outputs = Vec::with_capacity(drivers.len());
        for (index, driver) in drivers.into_iter().enumerate() {
            let (decisions, research) =
                driver
                    .finish()
                    .map_err(|error| PortfolioReplayError::Instance {
                        instance_id: instance_ids[index].clone(),
                        source: map_strategy_driver_error(error),
                    })?;
            outputs.push(PortfolioInstanceOutput {
                strategy_id: strategy_ids[index].clone(),
                instance_id: instance_ids[index].clone(),
                descriptor: descriptors[index].clone(),
                decisions,
                research,
            });
        }
        Ok(PortfolioBacktestResult {
            replay,
            instances: outputs,
            supervisor: state.into_output(),
        })
    }
}

fn mixed_direct_error<E: std::fmt::Display>(error: &StrategyDriverError<E>) -> String {
    match error {
        StrategyDriverError::Series(error) => error.to_string(),
        StrategyDriverError::SeriesView(error) => error.to_string(),
        StrategyDriverError::Analysis(error) => error.to_string(),
        StrategyDriverError::Strategy(error) => error.to_string(),
        StrategyDriverError::Runtime(error) => error.to_string(),
        StrategyDriverError::WarmupSignals { timestamp } => {
            format!("strategy emitted signals during warmup at {timestamp}")
        }
        StrategyDriverError::InvalidGeneratedSignal {
            signal_index,
            reason,
        } => format!("generated signal {signal_index} is invalid: {reason}"),
        StrategyDriverError::TickExecutionRequired { symbol, timestamp } => {
            format!("{symbol} requires tick execution at {timestamp}")
        }
    }
}

fn declared_series(
    requirements: &crate::strategy::StrategyRequirements,
) -> BTreeMap<String, BTreeSet<u64>> {
    let mut declared = BTreeMap::<String, BTreeSet<u64>>::new();
    for requirement in requirements.series() {
        declared
            .entry(requirement.symbol().to_owned())
            .or_default()
            .insert(requirement.timeframe().duration_seconds());
    }
    declared
}

/// Account-currency risk an Entry requests before its fill: known for monetary sizing, unknown for fixed lots.
fn requested_account_risk(
    sizing: Option<&SizingPolicy>,
    balance: f64,
    risk_multiplier: f64,
) -> Option<f64> {
    let risk = match sizing? {
        SizingPolicy::FixedRiskAmount { amount } => amount * risk_multiplier,
        SizingPolicy::BalanceRiskPercent { percent } => balance * percent / 100.0 * risk_multiplier,
        SizingPolicy::FixedLot { .. } => return None,
    };
    (risk.is_finite() && risk > 0.0).then_some(risk)
}
