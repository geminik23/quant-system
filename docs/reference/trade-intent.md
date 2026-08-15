# Trade intent and execution events

`quant-system-core` provides source-neutral, strategy-neutral, and venue-neutral domain contracts for desired trading actions and execution facts. These contracts are pure Rust library APIs. They do not perform catalog lookup, portfolio lookup, risk approval, order submission, persistence, or reconciliation IO.

Current backtest service endpoints still accept strict [`RawSignal`](raw-signal.md). The canonical contracts are additive foundations for future ingestion bridges, strategy runtimes, portfolio supervisors, replay adapters, execution gateways, and venue adapters.

## Modules

| Module | Responsibility |
|---|---|
| `qs_core::canonical` | Canonical UTC timestamps, validated identities, operating modes, execution capabilities, fractions, durations, and price distances |
| `qs_core::intent` | `TradeIntent`, desired action variants, constraints, provenance, identity comparison, `RawSignal` adaptation, and representable reverse projection |
| `qs_core::execution_events` | Immutable execution commands, dispatch attempts and reports, venue execution reports, exact fill/order/position facts, typed venue deduplication keys, and pure FutureQuote effect projection |

## Trade intent

A `TradeIntent` pins one `ResolvedInstrumentRef` containing the qualified instrument, catalog snapshot, and specification revision selected by the caller. The intent layer validates that reference but performs no catalog IO.

The envelope carries:

- a schema version and stable `TradeIntentId`;
- canonical UTC creation, effective, and optional expiration times;
- an optional expected-state reference and revision;
- producer identity, kind, correlation, and opaque source references;
- one desired economic action;
- operating-mode, slippage, age, capability, reduce-only, and supersession constraints.

`created_at` is producer logical creation time and `effective_at` is economic applicability time. Delayed imports and scheduled actions do not have one universal ordering relation. An expiration, when present, must be later than `effective_at`.

### Actions

| Action | Meaning |
|---|---|
| `Enter` | Request new exposure with an order preference, optional price references, target hints, and an unapproved risk request |
| `Reduce` | Reduce an already resolved position by exact quantity, positive fraction, or all remaining exposure |
| `Exit` | Exit an already resolved position |
| `ReplaceProtection` | Replace, clear, or move protection to breakeven |
| `ReplaceTargets` | Replace the complete desired target set |
| `AddTranche` | Add an explicit final quantity with a separate optional entry reference |
| `CancelEntry` | Cancel an already resolved pending entry |
| `FlattenScope` | Flatten an exact resolved position or another explicitly represented scope |

A risk request is not an approved size. Portfolio and risk consumers remain responsible for account guardrails, sizing, allocation, and capability enforcement.

## Stable identity

`TradeIntentId` and `ExecutionCommandId` use direct readable identities rather than hashes. A generated trade intent ID has the form `intent:{namespace}:{ordinal}`, and a generated execution command ID has the form `command:{intent_id}:{ordinal}`. Their validated bounds account for the largest valid namespace, nested intent ID, and decimal ordinal while retaining strict ASCII, control-character, and whitespace rejection.

A generated `TradeIntentId` derives only from the caller-supplied identity namespace and action ordinal. The final ID is assigned before intent validation, so self-supersession validation observes the actual identity. Reusing the same namespace and ordinal for different intent content produces the same ID and is classified as a conflict by direct typed equality after the ID check.

An `ExecutionCommandId` derives only from the intent ID and command ordinal. `ExecutionCommandEnvelope::with_deterministic_id` does not serialize the payload and does not return a serialization result. Creation time and payload are compared directly after the ID check, so changing either while reusing an intent ID and ordinal is a conflict. A transport retry uses a separate `CommandDispatchAttempt`; it does not mutate the immutable command or generate a replacement identity for the same operation.

Dispatch and execution reports do not carry generated report IDs. `CommandDispatchReport` identity is its intent ID, command ID, execution venue, and observation time; reports with matching coordinates compare their complete typed semantics, while reports with different coordinates are distinct. `ExecutionReport` identity is its `VenueEventDedupKey`. Report semantics ignore local `received_at` and optional intent and command correlation, allowing the same venue fact to be received again or enriched with later correlation without conflict. Reusing one sequenced venue key for a different venue fact is a conflict.

## RawSignal adaptation

`adapt_raw_signal` accepts a `RawSignal` and a `RawSignalAdaptationContext`. The context must already contain the pinned timestamp policy, instrument resolution, position or pending-entry targets, desired target state where required, provenance, constraints, and deterministic identity namespace.

The adapter:

- preserves Entry risk as an unapproved unit multiplier;
- preserves optional market reference prices and entry correlation;
- expands close, reduction, protection, cancellation, and bulk actions in deterministic resolved-target order;
- keeps `ScaleIn.size` as an explicit final quantity;
- projects target deltas to a complete desired replacement only when the caller supplies pinned current desired state;
- returns `AddRule` and `RemoveRule` as explicit compatibility-only outcomes;
- treats an empty caller-resolved bulk scope as a valid empty result.

The adapter does not resolve source updates or deletes into economic actions. It also does not read a catalog, position store, source-state store, or configuration file.

## Command and dispatch contracts

`ExecutionCommandEnvelope<T>` is the immutable identity envelope for a typed, gateway-owned payload. `CommandDispatchAttempt<T>` records an attempt number and dispatch time without changing command identity.

`CommandDispatchReport` records transport and gateway lifecycle observations:

- transport acknowledged;
- transport failed;
- unknown outcome;
- reconciliation required.

A transport acknowledgement is not venue acceptance. An unknown outcome requires reconciliation before blind resubmission of effectful work.

## Venue execution reports

`ExecutionReport` contains venue economic facts and may be uncorrelated when activity is discovered after restart or originated outside the application. Optional intent and command IDs can be attached when known or added later without changing equality or deduplication. `VenueEventDedupKey` excludes those correlations and `received_at`. Its `Sequenced` variant contains venue, instrument, and venue sequence identity; its `Unsequenced` variant contains venue, instrument, event time, event, and optional opaque payload reference.

Supported facts include:

- venue acceptance or rejection;
- partial and final incremental fills;
- cancellation and expiration;
- protection and target changes;
- position changes and closes;
- reconciliation snapshots.

Canonical facts use exact instrument-domain prices, quantities, assets, and signed nonzero fee or rebate amounts. Partial fills require positive remaining quantity, final fills require zero remaining quantity, and nested order, fill, target, fee, and position facts are validated during both authoring and deserialization.

## Not provided

These contracts do not provide:

- a portfolio supervisor or risk governor;
- approved sizing or allocation;
- a durable outbox, inbox, or intent store;
- a live execution gateway;
- venue-specific command translation;
- broker or exchange connectivity;
- reconciliation algorithms;
- automatic committed-ingestion-batch projection;
- automatic migration of current backtest endpoints from `RawSignal`.

Applications adopting these contracts must implement those responsibilities in their owning layers and preserve the distinction between desired intent, dispatch lifecycle, and venue economic facts.
