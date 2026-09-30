//! Synchronous portfolio supervision.
//!
//! A [`PortfolioSupervisor`] sits between the strategies that want new exposure and the stage that executes it. It sees each Entry or scale-in request together with facts about the account, applies declared [`RiskPolicy`] values, and approves or rejects the request. It never blocks a close, a partial close, a stop move, or a cancellation, because a supervisor must not prevent risk reduction.
//!
//! The crate owns no clock, IO, or async runtime. A historical replay and a live runtime supply the same facts and receive the same verdicts.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{Duration, NaiveDateTime, NaiveTime};
use qs_core::types::Side;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Largest number of policies or groups one supervisor accepts.
pub const MAX_POLICIES: usize = 64;
pub const MAX_GROUPS: usize = 64;
/// Largest number of symbols one correlation group lists.
pub const MAX_GROUP_SYMBOLS: usize = 256;
/// Longest group identifier in bytes.
pub const MAX_GROUP_ID_BYTES: usize = 64;

/// Tolerance for comparing sums of account-currency risk against a cap.
const RISK_EPSILON: f64 = 1e-9;

/// Symbols the owner declares to be one bet, so a cap can bound their combined risk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorrelationGroup {
    pub id: String,
    pub symbols: BTreeSet<String>,
}

/// One portfolio rule.
///
/// Risk figures are account-currency amounts lost if a position's protective stop fills: what a monetary sizing policy requests before a fill and what the position's initial risk reports after it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RiskPolicy {
    /// At most `limit` positions open or pending at once, counting approved entries that have not reached the market yet.
    MaxOpenPositions { limit: usize },
    /// At most `limit` positions open or pending per symbol, counted the same way.
    MaxOpenPerSymbol { limit: usize },
    /// The positions in `group` together may carry at most `max_group_risk` of risk; an entry that would exceed it is rejected.
    GroupRiskCap { group: String, max_group_risk: f64 },
    /// After the day's realized loss reaches `max_loss`, new exposure is rejected until the next `reset_at_utc`.
    DailyLossHalt {
        max_loss: LossLimit,
        reset_at_utc: NaiveTime,
    },
    /// Once equity falls `max_drawdown_percent` percent below its peak, new exposure is rejected for the rest of the run, and `HaltAndCloseAll` also closes everything.
    KillSwitch {
        max_drawdown_percent: f64,
        action: HaltAction,
    },
}

impl RiskPolicy {
    /// Stable name used in verdict reasons and supervisor events.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::MaxOpenPositions { .. } => "max_open_positions",
            Self::MaxOpenPerSymbol { .. } => "max_open_per_symbol",
            Self::GroupRiskCap { .. } => "group_risk_cap",
            Self::DailyLossHalt { .. } => "daily_loss_halt",
            Self::KillSwitch { .. } => "kill_switch",
        }
    }
}

/// How large a day's realized loss may become.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum LossLimit {
    /// Percent of the balance at the day's reset instant; `2.0` means two percent.
    AccountPercent(f64),
    /// Account-currency amount.
    Amount(f64),
    /// Sum of realized R over positions fully closed since the reset; `3.0` halts at minus three R.
    RiskMultiples(f64),
}

/// What a kill switch does when it trips.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HaltAction {
    /// Reject new exposure and cancel pending orders.
    Halt,
    /// Also close every open position at the next quote.
    HaltAndCloseAll,
}

/// Whether a request adds a position or grows an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntentKind {
    Entry,
    ScaleIn,
}

/// One request for new exposure as the supervisor sees it.
#[derive(Debug, Clone, PartialEq)]
pub struct ExposureIntent<'a> {
    pub symbol: &'a str,
    pub side: Side,
    pub kind: IntentKind,
    /// Account-currency risk the request asks for, or `None` when it cannot be known before the fill, such as a fixed-lot entry or a scale-in of an explicit quantity.
    pub requested_risk: Option<f64>,
}

/// One position, pending order, or approved request the supervisor counts.
#[derive(Debug, Clone, PartialEq)]
pub struct ExposureFact {
    pub symbol: String,
    pub side: Side,
    /// Account-currency risk, or `None` when it is unknown.
    pub risk: Option<f64>,
}

/// Account state at the moment of a review or boundary.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PortfolioFacts<'a> {
    pub now: NaiveDateTime,
    /// Realized balance, net of every cost already charged.
    pub balance: f64,
    /// Current drawdown of marked equity from its peak as a fraction, or `None` while no mark exists; a kill switch stays inactive while it is `None`.
    pub drawdown_fraction: Option<f64>,
    /// Sum of realized R over positions fully closed at or after [`PortfolioSupervisor::day_start`].
    pub day_realized_r: f64,
    pub open: &'a [ExposureFact],
    pub pending: &'a [ExposureFact],
    /// Approved requests that have not reached the market yet.
    pub reserved: &'a [ExposureFact],
}

/// Outcome of one review.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum Verdict {
    Approve,
    Reject { policy: String, reason: String },
}

impl Verdict {
    fn reject(policy: &str, reason: impl Into<String>) -> Self {
        Self::Reject {
            policy: policy.to_owned(),
            reason: reason.into(),
        }
    }

    pub const fn is_approved(&self) -> bool {
        matches!(self, Self::Approve)
    }
}

/// A bulk action a halt asks the executing stage to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HaltCommand {
    CancelAllPending,
    CloseAll,
}

/// A period during which new exposure was rejected.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HaltInterval {
    pub policy: String,
    pub from: NaiveDateTime,
    /// End of the halt, or `None` when it was still in force when the run ended.
    pub to: Option<NaiveDateTime>,
}

/// A supervisor configuration the owner cannot run.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum RiskConfigError {
    #[error("too many {what}: {count} exceeds {max}")]
    TooMany {
        what: &'static str,
        count: usize,
        max: usize,
    },
    #[error(
        "group id '{id}' must be 1 to {MAX_GROUP_ID_BYTES} bytes of ASCII letters, digits, '_', '-', or '.'"
    )]
    InvalidGroupId { id: String },
    #[error("group '{id}' is declared more than once")]
    DuplicateGroup { id: String },
    #[error("group '{id}' must list between 1 and {MAX_GROUP_SYMBOLS} non-empty symbols")]
    InvalidGroupSymbols { id: String },
    #[error("{policy} refers to undeclared group '{group}'")]
    UnknownGroup { policy: &'static str, group: String },
    #[error("{policy}: {field} must be {requirement}, got {value}")]
    InvalidValue {
        policy: &'static str,
        field: &'static str,
        requirement: &'static str,
        value: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
struct DailyState {
    day_start: NaiveDateTime,
    day_start_balance: f64,
    halted: bool,
}

/// Portfolio risk supervisor for one account.
#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioSupervisor {
    policies: Vec<RiskPolicy>,
    groups: BTreeMap<String, BTreeSet<String>>,
    daily: Option<DailyState>,
    /// Indices of the kill-switch policies that have tripped; each one trips once and stays tripped.
    tripped_kill_switches: BTreeSet<usize>,
    close_all_issued: bool,
    intervals: Vec<HaltInterval>,
}

impl PortfolioSupervisor {
    /// Validate the policies and groups and build a supervisor with no halt in force.
    pub fn new(
        policies: Vec<RiskPolicy>,
        groups: Vec<CorrelationGroup>,
    ) -> Result<Self, RiskConfigError> {
        if policies.len() > MAX_POLICIES {
            return Err(RiskConfigError::TooMany {
                what: "policies",
                count: policies.len(),
                max: MAX_POLICIES,
            });
        }
        if groups.len() > MAX_GROUPS {
            return Err(RiskConfigError::TooMany {
                what: "groups",
                count: groups.len(),
                max: MAX_GROUPS,
            });
        }
        let mut declared = BTreeMap::new();
        for group in groups {
            if !valid_group_id(&group.id) {
                return Err(RiskConfigError::InvalidGroupId { id: group.id });
            }
            if group.symbols.is_empty()
                || group.symbols.len() > MAX_GROUP_SYMBOLS
                || group.symbols.iter().any(|symbol| symbol.trim().is_empty())
            {
                return Err(RiskConfigError::InvalidGroupSymbols { id: group.id });
            }
            if declared.contains_key(&group.id) {
                return Err(RiskConfigError::DuplicateGroup { id: group.id });
            }
            declared.insert(group.id, group.symbols);
        }
        for policy in &policies {
            validate_policy(policy, &declared)?;
        }
        let resets = policies
            .iter()
            .filter_map(|policy| match policy {
                RiskPolicy::DailyLossHalt { reset_at_utc, .. } => Some(*reset_at_utc),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        if resets.len() > 1 {
            return Err(RiskConfigError::InvalidValue {
                policy: "daily_loss_halt",
                field: "reset_at_utc",
                requirement: "the same instant for every daily loss policy",
                value: resets
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            });
        }
        Ok(Self {
            policies,
            groups: declared,
            daily: None,
            tripped_kill_switches: BTreeSet::new(),
            close_all_issued: false,
            intervals: Vec::new(),
        })
    }

    pub fn policies(&self) -> &[RiskPolicy] {
        &self.policies
    }

    /// Whether any policy caps the risk of a group, which requires every counted risk to be measurable.
    pub fn caps_group_risk(&self) -> bool {
        self.policies
            .iter()
            .any(|policy| matches!(policy, RiskPolicy::GroupRiskCap { .. }))
    }

    /// Start of the current daily-loss window, or `None` when no daily policy is declared or no time has been observed.
    pub fn day_start(&self) -> Option<NaiveDateTime> {
        self.daily.as_ref().map(|daily| daily.day_start)
    }

    /// Whether new exposure is being rejected because of a halt.
    pub fn halted(&self) -> bool {
        !self.tripped_kill_switches.is_empty()
            || self.daily.as_ref().is_some_and(|daily| daily.halted)
    }

    /// Observe the account before anything happens at `now`, which rolls the daily window when a reset instant has passed.
    pub fn begin(&mut self, now: NaiveDateTime, balance: f64) {
        let Some(reset) = self.daily_reset() else {
            return;
        };
        let day_start = latest_reset_at_or_before(now, reset);
        match &mut self.daily {
            Some(daily) if daily.day_start == day_start => {}
            Some(daily) => {
                if daily.halted {
                    close_interval(&mut self.intervals, "daily_loss_halt", day_start);
                }
                *daily = DailyState {
                    day_start,
                    day_start_balance: balance,
                    halted: false,
                };
            }
            None => {
                self.daily = Some(DailyState {
                    day_start,
                    day_start_balance: balance,
                    halted: false,
                });
            }
        }
    }

    /// Evaluate the halt policies against the facts at a boundary and return the bulk actions a new halt asks for, in execution order.
    pub fn on_boundary(&mut self, facts: &PortfolioFacts<'_>) -> Vec<HaltCommand> {
        self.begin(facts.now, facts.balance);
        let mut commands = Vec::new();
        let mut halt_started = false;
        for (index, policy) in self.policies.iter().enumerate() {
            match policy {
                RiskPolicy::DailyLossHalt { max_loss, .. } => {
                    let Some(daily) = self.daily.as_mut() else {
                        continue;
                    };
                    if daily.halted {
                        continue;
                    }
                    let breached = match max_loss {
                        LossLimit::AccountPercent(percent) => {
                            daily.day_start_balance - facts.balance
                                >= daily.day_start_balance * percent / 100.0 - RISK_EPSILON
                        }
                        LossLimit::Amount(amount) => {
                            daily.day_start_balance - facts.balance >= amount - RISK_EPSILON
                        }
                        LossLimit::RiskMultiples(multiples) => {
                            -facts.day_realized_r >= multiples - RISK_EPSILON
                        }
                    };
                    if breached {
                        daily.halted = true;
                        halt_started = true;
                        self.intervals.push(HaltInterval {
                            policy: policy.name().to_owned(),
                            from: facts.now,
                            to: None,
                        });
                    }
                }
                RiskPolicy::KillSwitch {
                    max_drawdown_percent,
                    action,
                } => {
                    // Several kill switches form tiers, such as halting at one drawdown and closing everything at a deeper one, so each trips on its own.
                    if self.tripped_kill_switches.contains(&index) {
                        continue;
                    }
                    let Some(drawdown) = facts.drawdown_fraction else {
                        continue;
                    };
                    if drawdown * 100.0 >= max_drawdown_percent - RISK_EPSILON {
                        let first_trip = self.tripped_kill_switches.is_empty();
                        self.tripped_kill_switches.insert(index);
                        halt_started |= first_trip;
                        self.intervals.push(HaltInterval {
                            policy: policy.name().to_owned(),
                            from: facts.now,
                            to: None,
                        });
                        if *action == HaltAction::HaltAndCloseAll && !self.close_all_issued {
                            self.close_all_issued = true;
                            commands.push(HaltCommand::CloseAll);
                        }
                    }
                }
                _ => {}
            }
        }
        if halt_started {
            commands.insert(0, HaltCommand::CancelAllPending);
        }
        commands
    }

    /// Approve or reject one request for new exposure against the facts at the moment of review.
    pub fn review(&self, facts: &PortfolioFacts<'_>, intent: &ExposureIntent<'_>) -> Verdict {
        if !self.tripped_kill_switches.is_empty() {
            return Verdict::reject(
                "kill_switch",
                "new exposure is halted for the rest of the run",
            );
        }
        if let Some(daily) = self.daily.as_ref().filter(|daily| daily.halted) {
            return Verdict::reject(
                "daily_loss_halt",
                format!(
                    "new exposure is halted until the reset after {}",
                    daily.day_start
                ),
            );
        }
        let counted = || facts.open.iter().chain(facts.pending).chain(facts.reserved);
        for policy in &self.policies {
            match policy {
                RiskPolicy::MaxOpenPositions { limit } if intent.kind == IntentKind::Entry => {
                    let count = counted().count();
                    if count >= *limit {
                        return Verdict::reject(
                            policy.name(),
                            format!(
                                "{count} positions open, pending, or approved of limit {limit}"
                            ),
                        );
                    }
                }
                RiskPolicy::MaxOpenPerSymbol { limit } if intent.kind == IntentKind::Entry => {
                    let count = counted()
                        .filter(|fact| fact.symbol == intent.symbol)
                        .count();
                    if count >= *limit {
                        return Verdict::reject(
                            policy.name(),
                            format!(
                                "{count} positions open, pending, or approved on {} of limit {limit}",
                                intent.symbol
                            ),
                        );
                    }
                }
                RiskPolicy::GroupRiskCap {
                    group,
                    max_group_risk,
                } => {
                    let symbols = &self.groups[group];
                    if !symbols.contains(intent.symbol) {
                        continue;
                    }
                    let Some(requested) = intent.requested_risk else {
                        return Verdict::reject(
                            policy.name(),
                            format!(
                                "risk_unmeasurable: the request's risk is unknown before the fill, so group '{group}' cannot be capped"
                            ),
                        );
                    };
                    let mut carried = 0.0;
                    for fact in counted().filter(|fact| symbols.contains(&fact.symbol)) {
                        match fact.risk {
                            Some(risk) => carried += risk,
                            None => {
                                return Verdict::reject(
                                    policy.name(),
                                    format!(
                                        "group_risk_unmeasurable: a {} position in group '{group}' has unknown risk",
                                        fact.symbol
                                    ),
                                );
                            }
                        }
                    }
                    if carried + requested > max_group_risk + RISK_EPSILON {
                        return Verdict::reject(
                            policy.name(),
                            format!(
                                "group '{group}' carries {carried} and the request adds {requested}, above the cap of {max_group_risk}"
                            ),
                        );
                    }
                }
                _ => {}
            }
        }
        Verdict::Approve
    }

    /// Every halt interval in start order; an interval without an end was still in force when the supervisor finished.
    pub fn finish(self) -> Vec<HaltInterval> {
        self.intervals
    }

    /// Halt intervals recorded so far, with those still in force left open.
    pub fn intervals(&self) -> &[HaltInterval] {
        &self.intervals
    }

    fn daily_reset(&self) -> Option<NaiveTime> {
        self.policies.iter().find_map(|policy| match policy {
            RiskPolicy::DailyLossHalt { reset_at_utc, .. } => Some(*reset_at_utc),
            _ => None,
        })
    }
}

fn close_interval(intervals: &mut [HaltInterval], policy: &str, at: NaiveDateTime) {
    if let Some(interval) = intervals
        .iter_mut()
        .rev()
        .find(|interval| interval.policy == policy && interval.to.is_none())
    {
        interval.to = Some(at);
    }
}

fn latest_reset_at_or_before(now: NaiveDateTime, reset: NaiveTime) -> NaiveDateTime {
    let today = now.date().and_time(reset);
    if today <= now {
        today
    } else {
        today - Duration::days(1)
    }
}

fn valid_group_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= MAX_GROUP_ID_BYTES
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn validate_policy(
    policy: &RiskPolicy,
    groups: &BTreeMap<String, BTreeSet<String>>,
) -> Result<(), RiskConfigError> {
    let invalid = |field: &'static str, requirement: &'static str, value: String| {
        Err(RiskConfigError::InvalidValue {
            policy: policy.name(),
            field,
            requirement,
            value,
        })
    };
    match policy {
        RiskPolicy::MaxOpenPositions { limit } | RiskPolicy::MaxOpenPerSymbol { limit } => {
            if *limit == 0 {
                return invalid("limit", "at least 1", limit.to_string());
            }
        }
        RiskPolicy::GroupRiskCap {
            group,
            max_group_risk,
        } => {
            if !groups.contains_key(group) {
                return Err(RiskConfigError::UnknownGroup {
                    policy: policy.name(),
                    group: group.clone(),
                });
            }
            if !(max_group_risk.is_finite() && *max_group_risk > 0.0) {
                return invalid(
                    "max_group_risk",
                    "finite and positive",
                    max_group_risk.to_string(),
                );
            }
        }
        RiskPolicy::DailyLossHalt { max_loss, .. } => {
            let (field, value) = match max_loss {
                LossLimit::AccountPercent(value) => ("account_percent", *value),
                LossLimit::Amount(value) => ("amount", *value),
                LossLimit::RiskMultiples(value) => ("risk_multiples", *value),
            };
            let valid = value.is_finite()
                && value > 0.0
                && (!matches!(max_loss, LossLimit::AccountPercent(_)) || value <= 100.0);
            if !valid {
                return invalid(
                    field,
                    "finite and positive, and a percent at most 100",
                    value.to_string(),
                );
            }
        }
        RiskPolicy::KillSwitch {
            max_drawdown_percent,
            ..
        } => {
            if !(max_drawdown_percent.is_finite()
                && *max_drawdown_percent > 0.0
                && *max_drawdown_percent <= 100.0)
            {
                return invalid(
                    "max_drawdown_percent",
                    "greater than 0 and at most 100",
                    max_drawdown_percent.to_string(),
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
