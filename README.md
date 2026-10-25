**Mock project for a demo. All data is synthetic. Not affiliated with any vehicle manufacturer.**

# data-contracts

Schemas, topic registry and a deterministic synthetic data generator for a mock vehicle-to-cloud telemetry
platform. The Scala Spark jobs, the Rust ports and the parity harness all build against this repo. See
[docs/PROJECT_CONTEXT.md](docs/PROJECT_CONTEXT.md) for the wider project.

```
contracts/common.schema.json           shared $defs (envelope fields, VIN, timestamps)
contracts/topics/<topic>.schema.json   one JSON Schema (Draft 2020-12) per topic or sink
contracts/topics.yaml                  topic registry
generator/gen.py                       deterministic generator (Python standard library only)
fixtures/sample/                       committed sample: --seed 42 --vehicles 20 --hours 6
tests/                                 pytest suite
```

## Run

Python 3.11.

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q                                   # about 5 seconds

python generator/gen.py                               # defaults: --seed 42 --vehicles 500 --hours 24
                                                      #   --start 2026-10-01T00:00:00.000Z --out fixtures/
python generator/gen.py --seed 42 --vehicles 20 --hours 6 --out fixtures/sample   # regenerate the sample
```

The generator writes one JSONL file per input topic and a `MANIFEST.json` with the arguments, record counts per
event type, duplicate and late counts, maximum lateness and the SHA-256 of each file. The same arguments always
produce byte-identical files: all randomness comes from `random.Random(seed)`, and the generator never reads the
clock or the network. Each line is `json.dumps(record, sort_keys=True, separators=(",", ":"))`.

## Topics

| Topic | Direction | Key | Partitions | Producer | Consumers | Sensitive fields |
|---|---|---|---|---|---|---|
| `vehicle.charging.events.v1` | input | `vin` | 6 | vehicle | charging-session-builder | `vin` (vehicle-id), `charger_lat` (precise-location), `charger_lon` (precise-location) |
| `vehicle.ota.status.v1` | input | `vin` | 6 | vehicle | ota-status-tracker | `vin` (vehicle-id) |
| `vehicle.battery.health.v1` | input | `vin` | 6 | vehicle | battery-health-aggregator | `vin` (vehicle-id) |
| `vehicle.diagnostics.dtc.v1` | input | `vin` | 6 | vehicle | diagnostics-dtc-router | `vin` (vehicle-id), `lat` (precise-location), `lon` (precise-location) |
| `charging.sessions.v1` | output | `vin` | 6 | charging-session-builder | service, energy-insights | `session_id` (vehicle-id), `vin` (vehicle-id), `charger_lat` (precise-location), `charger_lon` (precise-location) |
| `ota.status.latest.v1` | output | `vin\|campaign_id` | 6 | ota-status-tracker | ota-orchestrator, service | `vin` (vehicle-id) |
| `battery.health.hourly` | output | none (Parquet sink) | none | battery-health-aggregator | warranty-analytics | `vin` (vehicle-id) |
| `service.dtc.alerts.v1` | output | `vin` | 6 | diagnostics-dtc-router | service-scheduling | `alert_id` (vehicle-id), `vin` (vehicle-id), `lat` (coarse-location), `lon` (coarse-location) |

All Kafka topics use cleanup policy `delete`, except `ota.status.latest.v1`, which is `compact`. Its key is
`vin` and `campaign_id` joined by `|`, so compaction keeps one record per vehicle and campaign.
`battery.health.hourly` is written as Parquet to `s3a://telemetry-lake/battery_health_hourly`, partitioned by
`date`. Kafka topics declare `x-partitioner: murmur2` (the Java client default); Rust clients on librdkafka must
set `partitioner=murmur2_random` to put each VIN on the same partition.

The four input topics share one envelope: `event_id`, `vin`, `event_time`, `event_type`, `producer_seq`,
`schema_version`, `vehicle_model`, `fleet`, `market`, `firmware_version`. A `heartbeat` event carries the envelope
only, with every domain field null. On every other event type, every domain field is non-null, except
`failure_code`, which is non-null if and only if `state = failed`.

## Privacy tags

Every property, including nested ones, has exactly one `x-privacy` tag. `sensitive_fields` in `topics.yaml` lists
every field whose tag is not `none`, and a test checks that it matches the schemas.

| Tag | Meaning |
|---|---|
| `none` | Not sensitive on its own. |
| `vehicle-id` | Identifies one vehicle: the VIN, or an ID derived from it (`session_id`, `alert_id`). |
| `precise-location` | Latitude or longitude with 5 decimals (about 1 m). Can reveal where an owner lives, for example a home charger. `x-precision: 5`. |
| `coarse-location` | Latitude or longitude rounded to 2 decimals (about 1 km). `x-precision: 2`. |

## Anomaly injection

Each input file is generated clean and in event-time order, then disturbed. Heartbeats are never duplicated or
delayed. `event_time` always stays the true time.

- **Duplicates.** 2% of non-heartbeat events (an exact quota per topic) are emitted again as byte-identical
  copies, with the same `event_id`, arriving 1 to 60 seconds after the original.
- **Late arrivals.** 3% of non-heartbeat events (an exact quota per topic) arrive after at least one event with a
  later `event_time`, by at most 300 seconds. Lateness is measured across the whole topic, the way a Spark
  watermark sees it. `MANIFEST.json` reports `late` (topic-wide) and `late_same_vin` (the subset that also arrives
  after a later event from the same VIN).
- **Missing `charge_stop`.** About 3% of charging sessions end without a `charge_stop` and close by gap timeout.
- **Warranty signal.** 1% of vehicles (at least one) have a cell voltage delta that rises steadily to over 60 mV.
  Their VINs are listed in `MANIFEST.json` under `warranty_signal_vins`.
- **Heartbeats.** For every VIN on every input topic, one heartbeat with `event_time` = that VIN's last
  non-heartbeat `event_time` + 2 hours (window end + 2 hours if the VIN has no events on the topic). It is the
  VIN's last line in the file, and its `producer_seq` continues the VIN's sequence.

## Derived IDs

Both IDs are UUIDv5 (RFC 9562) in the namespace `f5179d15-0fd7-5b5f-bf53-0852eb22d3d5`, which is
`UUIDv5(NAMESPACE_DNS, "contracts.telemetry.example")`. Names are UTF-8, and event IDs are in lowercase canonical
form.

- `charging.sessions.v1.session_id` = UUIDv5(namespace, `<vin>|<event_id of the charge_start>`)
- `service.dtc.alerts.v1.alert_id` = UUIDv5(namespace, `<source event_id>`)

```python
import uuid
NS = uuid.UUID("f5179d15-0fd7-5b5f-bf53-0852eb22d3d5")
session_id = str(uuid.uuid5(NS, f"{vin}|{charge_start_event_id}"))
alert_id = str(uuid.uuid5(NS, dtc_event_id))
```

## Rules for output jobs

The full rules are in each output schema's `description`. In short:

- Deduplicate by `event_id` first. Use event time only; no output field may come from a processing-time clock.
  Allow at least 300 seconds of lateness.
- A charging session closes at its first `charge_stop`, or by `gap_timeout` when the VIN's next event on the topic
  is more than 30 minutes later; `end_time` is then the last event's time.
- "Latest" OTA status means the highest `producer_seq` per `vin|campaign_id`.
- DTC alerts are emitted only for `event_type = dtc`, `severity >= 3` and `status = active`, with coordinates
  rounded HALF_UP to 2 decimals.
- Rounding is HALF_UP on exact decimal values, never binary floating point. Example: 2.675 rounds to 2.68.

## Decimal places

JSON Schema `multipleOf` fails on valid decimals under floating point (`57.3` is not a `multipleOf: 0.1`), so
decimal places are declared with `x-decimals` (maximum places) and `x-precision` (exact places for coordinates),
and the tests enforce them on every generated record. Precise coordinates always print with exactly 5 decimals.
