# RawSignal JSONL reference

`RawSignal` is the strict normalized input accepted by the current replay path. A JSONL file contains one action object per line in timestamp order.

Top-level action objects reject unknown fields. Timestamps accept UTC-naive ISO values such as `2026-01-15T10:00:00`, space-separated values, date-only values at UTC midnight, or RFC 3339 values with a known offset; offset values are normalized to UTC without consulting the OS timezone. Surrounding whitespace, timezone names, `24:00:00`, leap seconds, trailing text, and RFC 3339 `-00:00` offsets are rejected.

## Entry

```json
{"action":"Entry","ts":"2026-01-15T10:00:00","symbol":"EURUSD","side":"Buy","order_type":"Market","price":null,"risk":1.0,"stoploss":1.0950,"targets":[1.1050],"group":"example","trade_id":"example-1"}
```

| Field | Requirement |
|---|---|
| `symbol` | Must resolve consistently with imported data and symbol metadata |
| `side` | `Buy` or `Sell` |
| `order_type` | Current engine order type such as `Market`, `Limit`, or `Stop` |
| `price` | `null` for a market entry; finite positive price for priced entries |
| `risk` | Required finite positive multiplier applied to the selected sizing policy |
| `stoploss` | Optional for fixed-lot sizing; required for monetary risk sizing |
| `targets` | Optional ordered target prices |
| `group` | Optional reporting and bulk-management tag |
| `trade_id` | Optional application identity; required when later actions use `ByTradeId` |

Entry does not accept `size`. The client must provide exactly one sizing policy when any Entry is present. `ScaleIn.size` is different: it is already a concrete final quantity.

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
- Stop and target geometry must be valid for the resolved entry side and price.
- Management actions that cannot resolve a position are recorded as skipped rather than reinterpreted.
- The wire format is strict and currently unversioned. Producers should test their serialized fixtures against the matching workspace release.
