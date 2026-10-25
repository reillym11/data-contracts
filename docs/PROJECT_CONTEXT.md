# Project context

**Mock project for a demo. All data is synthetic. Not affiliated with any vehicle manufacturer.**

## What we are building

A mock vehicle-to-cloud telemetry platform for an electric vehicle maker, used in a product demo. Every VIN,
location and reading is generated. Nothing imitates a real company's code, real VINs, real people or real home
addresses. Synthetic VINs start with `SYN` so they can never collide with a real 17-character VIN prefix.

## The four projects

| # | Project | Status | Role |
|---|---|---|---|
| 1 | `data-contracts` (this one) | phase 1 | Topic schemas, topic registry, deterministic synthetic generator. Every other project depends on it. |
| 2 | `telemetry-spark-jobs` | later | Legacy Scala Spark Structured Streaming jobs that read the input topics and write the output topics. |
| 3 | `telemetry-rs` | later | Rust ports of those jobs. |
| 4 | `telemetry-parity` | later | Replays these fixtures through the Scala and Rust jobs and compares outputs, duplicates, ordering and privacy. |

The demo is a migration story: a legacy Spark pipeline is ported to Rust, and the parity harness proves the port
is equivalent. That only works if the contracts leave nothing to interpretation.

## Why the contracts are this strict

Two independent implementations must produce byte-comparable output from the same input. Everything that could
differ between Scala and Rust is pinned down in the schemas:

- **Identity.** `session_id` and `alert_id` are UUIDv5 values over documented names in a fixed namespace, so both
  jobs compute the same IDs without coordination.
- **Time.** Every derivation uses event time. No output field may come from a processing-time or wall-clock
  source. Input lateness is bounded at 300 seconds, so a job that allows at least 300 seconds of lateness sees
  every event.
- **Duplicates.** Inputs contain exact duplicate copies. Jobs deduplicate by `event_id` before doing anything else.
- **Ordering.** "Latest" means highest `producer_seq`, never "last to arrive".
- **Session boundaries.** Sessions close at `charge_stop` or after a 30 minute gap in event time. Every VIN gets a
  final heartbeat 2 hours after its last event, so open sessions and windows always close deterministically.
- **Numbers.** Decimal places are fixed per field (`x-decimals`, `x-precision`). Derived values use HALF_UP rounding
  on exact decimal values. Binary floating point rounding (for example Rust's `f64::round` on 2.675) is not allowed.
- **Partitioning.** Kafka topics declare `x-partitioner: murmur2`, the Java client default. Rust clients on
  librdkafka must set `partitioner=murmur2_random`, otherwise the same VIN lands on a different partition.
- **Types.** snake_case names and no unions other than `T | null`, so Rust serde and Spark map every field cleanly.

## Why privacy tags are machine-checkable

Every property carries exactly one `x-privacy` tag. The parity harness and CI can then check, without a human,
that no output leaks a field at a finer level than its contract allows. The main case is location: input
charging and DTC events carry precise coordinates (5 decimals, about 1 m), which can reveal where an owner lives
when the vehicle charges at home. `service.dtc.alerts.v1` is consumed by service scheduling, which only needs a
rough area, so it carries coarse coordinates (2 decimals, about 1 km).

## Data flow

```
vehicle ──► vehicle.charging.events.v1  ──► charging-session-builder  ──► charging.sessions.v1
vehicle ──► vehicle.ota.status.v1       ──► ota-status-tracker        ──► ota.status.latest.v1 (compacted)
vehicle ──► vehicle.battery.health.v1   ──► battery-health-aggregator ──► battery.health.hourly (Parquet)
vehicle ──► vehicle.diagnostics.dtc.v1  ──► diagnostics-dtc-router    ──► service.dtc.alerts.v1
```

## How later projects use this repo

- Treat `contracts/` as the source of truth. Generate Scala case classes and Rust structs from the schemas or
  check them against the schemas in tests; do not hand-copy field lists.
- Use `fixtures/sample/` for unit tests. Generate the full-size set (`python generator/gen.py`, 500 vehicles, 24
  hours) for integration and parity runs; it is reproducible from its arguments, so it is never committed.
- Compare outputs after sorting by key. Arrival order is deliberately scrambled by the late-arrival injection.
