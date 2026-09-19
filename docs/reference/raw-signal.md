# RawSignal JSONL reference

`RawSignal` is the strict normalized input accepted by the current replay path. A JSONL file contains one action object per line in timestamp order.

Top-level action objects reject unknown fields. Timestamps use naive ISO date-time values such as `2026-01-15T10:00:00`; the producer is responsible for aligning them with imported UTC market-data timestamps.

## Entry

```json
{"action":"Entry","ts":"2026-01-15T10:00:00","symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,"risk":1.0,"stoploss":1.0950,"targets":[1.1050],"group":"example","trade_id":"example-1","entry_class":"expanded"}
```

| Field | Requirement |
|---|---|
| `symbol` | Must resolve consistently with imported data and symbol metadata |
| `side` | `Buy` or `Sell` |
| `order_type` | Current engine order type such as `Market`, `Limit`, or `Stop` |
| `price` | Required finite positive requested price for Limit/Stop; optional finite positive sizing reference for Market |
| `risk` | Required finite positive multiplier applied to the selected sizing policy |
| `stoploss` | Optional for fixed-lot sizing; required for monetary risk sizing |
| `targets` | Optional ordered target prices |
| `group` | Optional reporting and bulk-management tag |
| `trade_id` | Optional application identity; required when later actions use `ByTradeId` |
| `entry_class` | Optional exact semantic class used by an explicit backtest class-to-profile route |

Entry does not accept `size`. The client must provide exactly one sizing policy when any Entry is present. FutureQuote Market sizing uses the actual fill price by default; a run may instead select the explicit Market `price` as the quantity reference, with automatic fill-price fallback when it is `null`. This choice does not change the actual fill, profile-relative levels, P&L, or actual fill-based risk. `ScaleIn.size` is different: it is already a concrete final quantity.

`entry_class` is optional and omitted from serialized JSON when absent. When present it must be non-empty, have no leading or trailing whitespace or control characters, and contain at most 128 UTF-8 bytes. Matching is case-sensitive and does not trim or normalize. It is not a profile name, group, or trade identity. A run must provide an exact route for every retained labeled Entry; unknown classes fail instead of using the default profile.

The field belongs to the direct strict RawSignal replay path. The frozen normalized-signal version 1 and committed-normalization envelope contracts do not carry it and reject labeled signals rather than silently dropping the class.

## Position references

Per-position actions use one of these shapes:

```jsonl
{"type":"ByTradeId","trade_id":"example-1"}
{"type":"AllOnSymbol","symbol":"EURUSD"}
{"type":"AllInGroup","group_id":"example"}
```

## Per-position actions

| Action | Additional fields |
|---|---|
| `Close` | `position` |
| `ClosePartial` | `position`, `ratio` |
| `ModifyStoploss` | `position`, `price` |
| `MoveStoplossToEntry` | `position` |
| `AddTarget` | `position`, `price`, `close_ratio` |
| `RemoveTarget` | `position`, `price` |
| `ModifyTarget` | `position`, `old_price`, `new_price` |
| `AddRule` | `position`, `rule` |
| `RemoveRule` | `position`, `rule_name` |
| `ScaleIn` | `position`, optional `price`, concrete `size` |
| `CancelPending` | `position` |

Examples:

```json
{"action":"Close","ts":"2026-01-15T10:01:00","position":{"type":"ByTradeId","trade_id":"example-1"}}
```



```json
{"action":"ScaleIn","ts":"2026-01-15T10:01:00","position":{"type":"ByTradeId","trade_id":"example-1"},"price":null,"size":0.01}
```

## Bulk actions

| Action | Additional fields |
|---|---|
| `CloseAllOf` | `symbol` |
| `CloseAll` | none |
| `CancelAllPending` | none |
| `ModifyAllStoploss` | `symbol`, `price` |
| `CloseAllInGroup` | `group_id` |
| `ModifyAllStoplossInGroup` | `group_id`, `price` |

## Validation notes

- Entry risk must be finite and greater than zero.
- Entry classes must satisfy the exact bounded-label contract and require an explicit route when present.
- Stop and target geometry must be valid for the resolved entry side and price.
- Management actions that cannot resolve a position are recorded as skipped rather than reinterpreted.
- The wire format is strict and currently unversioned. Producers should test their serialized fixtures against the matching workspace release.
