import json
import re
import uuid
from decimal import ROUND_HALF_UP, Decimal

import pytest
from jsonschema import Draft202012Validator

from conftest import INPUT_TOPICS, PRIVACY_TAGS, ROOT

ENVELOPE = ["event_id", "vin", "event_time", "event_type", "producer_seq", "schema_version",
            "vehicle_model", "fleet", "market", "firmware_version"]
NAMESPACE = uuid.UUID("f5179d15-0fd7-5b5f-bf53-0852eb22d3d5")
SNAKE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$")
CLOCK_WORDS = ("processing", "processed", "ingest", "received", "arrival", "created_at", "updated_at",
               "emitted", "wall", "now")


def all_documents(schemas):
    common, topics = schemas
    return {"common": common, **topics}


def walk_properties(node, path=""):
    """Yield (path, name, subschema) for every property definition, recursively.

    Conditional branches (if/then/else) only restrict properties defined elsewhere, so they are skipped.
    """
    if not isinstance(node, dict):
        return
    for name, sub in node.get("properties", {}).items():
        yield f"{path}.{name}", name, sub
        yield from walk_properties(sub, f"{path}.{name}")
    for key in ("$defs", "items"):
        if key == "$defs":
            for dname, d in node.get("$defs", {}).items():
                yield from walk_properties(d, f"{path}$defs.{dname}")
        elif isinstance(node.get(key), dict):
            yield from walk_properties(node[key], f"{path}[]")


def test_eight_topic_schemas(topic_schemas):
    assert len(topic_schemas) == 8


def test_check_schema(schemas):
    for name, s in all_documents(schemas).items():
        Draft202012Validator.check_schema(s)
        assert s["$schema"] == "https://json-schema.org/draft/2020-12/schema", name


def test_top_level_shape(topic_schemas):
    for name, s in topic_schemas.items():
        for key in ("$id", "title", "description", "properties", "required"):
            assert s.get(key), (name, key)
        assert s["type"] == "object"
        assert s["additionalProperties"] is False
        assert s["x-topic"] == name
        assert s["$id"].endswith(f"/topics/{name}.schema.json")
        # every field is always present; optional values are expressed as null
        assert s["required"] == list(s["properties"]), name


def test_nested_objects_are_closed(schemas):
    for name, s in all_documents(schemas).items():
        for path, _, sub in walk_properties(s):
            if sub.get("type") == "object" or "properties" in sub:
                assert sub.get("additionalProperties") is False, (name, path)


def test_every_property_has_one_valid_privacy_tag(schemas):
    count = 0
    for name, s in all_documents(schemas).items():
        for path, _, sub in walk_properties(s):
            assert sub.get("x-privacy") in PRIVACY_TAGS, (name, path, sub.get("x-privacy"))
            count += 1
    assert count > 100


def test_lat_lon_precision(schemas):
    for name, s in all_documents(schemas).items():
        for path, prop, sub in walk_properties(s):
            is_coord = prop in ("lat", "lon") or prop.endswith(("_lat", "_lon"))
            if is_coord:
                assert sub["x-privacy"] in ("precise-location", "coarse-location"), (name, path)
                expected = 5 if sub["x-privacy"] == "precise-location" else 2
                assert sub.get("x-precision") == expected, (name, path)
            else:
                assert "x-precision" not in sub, (name, path)
                assert sub["x-privacy"] not in ("precise-location", "coarse-location"), (name, path)


def test_snake_case_and_no_clock_fields(schemas):
    for name, s in all_documents(schemas).items():
        for path, prop, _ in walk_properties(s):
            assert SNAKE.match(prop), (name, path)
            assert not any(w in prop for w in CLOCK_WORDS), (name, path)


def test_only_nullable_unions(schemas):
    for name, s in all_documents(schemas).items():
        for path, _, sub in walk_properties(s):
            t = sub.get("type")
            if isinstance(t, list):
                assert len(t) == 2 and "null" in t, (name, path, t)
            assert not any(k in sub for k in ("oneOf", "anyOf")), (name, path)


def test_decimals_annotation_only_on_numbers(schemas):
    for name, s in all_documents(schemas).items():
        for path, _, sub in walk_properties(s):
            if "x-decimals" in sub:
                assert "number" in (sub["type"] if isinstance(sub["type"], list) else [sub["type"]]), path
            if "number" in str(sub.get("type")) and "x-precision" not in sub:
                assert "x-decimals" in sub, (name, path)


def test_input_topics_carry_exactly_the_envelope(schemas):
    common, topics = schemas
    assert list(common["$defs"]["envelope"]["properties"]) == ENVELOPE
    for t in INPUT_TOPICS:
        props = topics[t]["properties"]
        assert list(props)[: len(ENVELOPE)] == ENVELOPE, t
        for f in ENVELOPE:
            if f != "event_type":
                assert props[f]["$ref"] == f"../common.schema.json#/$defs/{'timestamp' if f == 'event_time' else f}"
        assert props["event_type"]["enum"][-1] == "heartbeat"


def test_parquet_sink_types(topic_schemas):
    s = topic_schemas["battery.health.hourly"]
    for prop, sub in s["properties"].items():
        assert sub.get("x-parquet-type"), prop


def test_deterministic_id_rules_are_documented(topic_schemas):
    assert str(NAMESPACE) in topic_schemas["charging.sessions.v1"]["description"]
    assert str(NAMESPACE) in topic_schemas["service.dtc.alerts.v1"]["description"]
    assert uuid.uuid5(uuid.NAMESPACE_DNS, "contracts.telemetry.example") == NAMESPACE


# ------------------------------------------------------------------ negative validation

def first(sample, topic, event_type, **match):
    for r in sample.records[topic]:
        if r["event_type"] == event_type and all(r[k] == v for k, v in match.items()):
            return dict(r)
    pytest.skip(f"no {event_type} {match} in sample")


def errors(validators, topic, rec):
    return list(validators[topic].iter_errors(rec))


@pytest.mark.parametrize("topic", INPUT_TOPICS)
def test_rejects_unknown_property(sample, validators, topic):
    rec = dict(sample.records[topic][0], ingest_time="2026-10-01T00:00:00.000Z")
    assert errors(validators, topic, rec)


@pytest.mark.parametrize("topic", INPUT_TOPICS)
def test_heartbeat_domain_fields_must_be_null(sample, validators, topic):
    hb = first(sample, topic, "heartbeat")
    assert not errors(validators, topic, hb)
    field = next(f for f in hb if f not in ENVELOPE)
    some_value = next(r[field] for r in sample.records[topic] if r[field] is not None) if any(
        r[field] is not None for r in sample.records[topic]) else "x"
    assert errors(validators, topic, dict(hb, **{field: some_value}))


@pytest.mark.parametrize("topic", INPUT_TOPICS)
def test_non_heartbeat_domain_fields_must_be_non_null(sample, validators, topic):
    rec = next((r for r in sample.records[topic] if r["event_type"] != "heartbeat"), None)
    if rec is None:
        pytest.skip("no events")
    field = next(f for f in rec if f not in ENVELOPE and f != "failure_code")
    assert errors(validators, topic, dict(rec, **{field: None}))


def test_failure_code_iff_failed(sample, validators):
    t = "vehicle.ota.status.v1"
    failed = first(sample, t, "ota_status", state="failed")
    downloaded = first(sample, t, "ota_status", state="downloaded")
    assert not errors(validators, t, failed) and not errors(validators, t, downloaded)
    assert errors(validators, t, dict(failed, failure_code=None))
    assert errors(validators, t, dict(downloaded, failure_code="E_TIMEOUT"))


@pytest.mark.parametrize("bad", ["2026-10-01T00:00:00Z", "2026-10-01T00:00:00.000+00:00",
                                 "2026-10-01 00:00:00.000Z", "nope"])
def test_timestamp_format_is_enforced(sample, validators, bad):
    t = "vehicle.battery.health.v1"
    assert errors(validators, t, dict(sample.records[t][0], event_time=bad))


@pytest.mark.parametrize("bad", ["SYN0000000000000I", "SYN000000000000O0", "SYNQ0000000000000", "1HG00000000000000",
                                 "SYN000000000000", "syn00000000000000"])
def test_vin_pattern_is_enforced(sample, validators, bad):
    t = "vehicle.battery.health.v1"
    assert errors(validators, t, dict(sample.records[t][0], vin=bad))


def test_power_cap_is_enforced(sample, validators):
    t = "vehicle.charging.events.v1"
    for ct, cap in (("home", 11.5), ("ac_l2", 19.2), ("dc_fast", 400.0)):
        rec = dict(first(sample, t, "charge_progress"), charger_type=ct, power_kw=cap)
        assert not errors(validators, t, rec), ct
        assert errors(validators, t, dict(rec, power_kw=cap + 0.1)), ct


# ------------------------------------------------------------------ output contracts accept derived records

def half_up(x, places):
    return float(Decimal(repr(x)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def test_half_up_rule_example():
    # binary floating point would give 2.67 for round(2.675, 2); the contract requires decimal HALF_UP
    assert half_up(2.675, 2) == 2.68
    assert half_up(37.12500, 2) == 37.13
    assert half_up(-122.12500, 2) == -122.13


def test_output_examples_validate(sample, validators):
    """Derive one record per output contract from the sample, following the documented rules."""
    ch = [r for r in sample.events("vehicle.charging.events.v1")]
    start = min((r for r in ch if r["event_type"] == "charge_start"), key=lambda r: r["event_time"])
    sess = sorted((r for r in ch if r["vin"] == start["vin"] and r["event_time"] >= start["event_time"]),
                  key=lambda r: r["event_time"])
    stop_i = next(i for i, r in enumerate(sess) if r["event_type"] == "charge_stop")
    sess = sess[: stop_i + 1]
    session = {
        "session_id": str(uuid.uuid5(NAMESPACE, f"{start['vin']}|{start['event_id']}")),
        "vin": start["vin"], "fleet": start["fleet"], "market": start["market"],
        "connector_type": start["connector_type"], "charger_type": start["charger_type"],
        "start_time": start["event_time"], "end_time": sess[-1]["event_time"], "close_reason": "charge_stop",
        "energy_added_kwh": sess[-1]["energy_added_kwh"], "peak_power_kw": max(r["power_kw"] for r in sess),
        "start_soc_pct": start["soc_pct"], "end_soc_pct": sess[-1]["soc_pct"], "event_count": len(sess),
        "charger_lat": start["charger_lat"], "charger_lon": start["charger_lon"],
    }
    assert not errors(validators, "charging.sessions.v1", session)

    dtc = next(r for r in sample.events("vehicle.diagnostics.dtc.v1")
               if r["severity"] >= 3 and r["status"] == "active")
    alert = {"alert_id": str(uuid.uuid5(NAMESPACE, dtc["event_id"])), "vin": dtc["vin"],
             "dtc_code": dtc["dtc_code"], "severity": dtc["severity"], "ecu": dtc["ecu"],
             "event_time": dtc["event_time"], "lat": half_up(dtc["lat"], 2), "lon": half_up(dtc["lon"], 2)}
    assert not errors(validators, "service.dtc.alerts.v1", alert)
    assert errors(validators, "service.dtc.alerts.v1", dict(alert, severity=2))

    ota = next(r for r in sample.events("vehicle.ota.status.v1") if r["state"] == "failed")
    latest = {"vin": ota["vin"], "campaign_id": ota["campaign_id"], "state": ota["state"],
              "target_version": ota["target_version"], "failure_code": ota["failure_code"],
              "last_event_id": ota["event_id"], "last_event_time": ota["event_time"]}
    assert not errors(validators, "ota.status.latest.v1", latest)

    b = next(r for r in sample.events("vehicle.battery.health.v1"))
    hour = b["event_time"][:13] + ":00:00.000Z"
    row = {"vin": b["vin"], "date": b["event_time"][:10], "window_start": hour,
           "window_end": hour[:11] + f"{int(hour[11:13]) + 1:02d}" + hour[13:], "sample_count": 1,
           "soh_pct_min": b["soh_pct"], "soh_pct_avg": b["soh_pct"],
           "cell_voltage_delta_mv_min": b["cell_voltage_delta_mv"],
           "cell_voltage_delta_mv_max": b["cell_voltage_delta_mv"], "pack_temp_c_max": b["pack_temp_c"]}
    assert not errors(validators, "battery.health.hourly", row)


def test_schema_files_are_canonical_json():
    for path in sorted((ROOT / "contracts").rglob("*.json")):
        text = path.read_text()
        assert text == json.dumps(json.loads(text), indent=2) + "\n", path
