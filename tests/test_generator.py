import ast
import hashlib
import json
import math
import re
import sys
from collections import Counter, defaultdict
from decimal import ROUND_HALF_UP, Decimal


import gen
from conftest import INPUT_TOPICS, ROOT, SAMPLE_ARGS, Run

VIN_RE = re.compile(r"^SYN[A-HJ-NPR-Z0-9]{14}$")
TWO_HOURS_MS = 2 * 3600 * 1000


def ms(ts):
    return gen.parse_ts(ts)


def decimals_of(x):
    text = json.dumps(x)
    return 0 if "." not in text else len(text.split(".")[1])


def km(a, b):
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def by_vin(records):
    out = defaultdict(list)
    for r in records:
        out[r["vin"]].append(r)
    return out


# ------------------------------------------------------------------ validity

def test_every_record_validates(run, validators):
    for topic in INPUT_TOPICS:
        v = validators[topic]
        for i, r in enumerate(run.records[topic]):
            errs = list(v.iter_errors(r))
            assert not errs, (topic, i, errs[0].message)


def test_decimal_places(run, topic_schemas):
    for topic in INPUT_TOPICS:
        props = topic_schemas[topic]["properties"]
        for r in run.records[topic]:
            for f, val in r.items():
                if val is None or isinstance(val, (str, bool)):
                    continue
                if "x-precision" in props[f]:
                    assert decimals_of(val) == props[f]["x-precision"], (topic, f, val)
                elif "x-decimals" in props[f]:
                    assert decimals_of(val) <= props[f]["x-decimals"], (topic, f, val)


def test_lines_are_compact_sorted_json(sample):
    for topic in INPUT_TOPICS:
        for line in sample.lines[topic]:
            assert line == json.dumps(json.loads(line), sort_keys=True, separators=(",", ":"))


# ------------------------------------------------------------------ determinism

def test_same_arguments_same_bytes(tmp_path):
    args = dict(seed=7, vehicles=15, hours=4, start="2026-10-01T00:00:00.000Z")
    a = gen.generate(out=str(tmp_path / "a"), **args)
    b = gen.generate(out=str(tmp_path / "b"), **args)
    assert a == b
    for f in ["MANIFEST.json", *(f"{t}.jsonl" for t in INPUT_TOPICS)]:
        assert (tmp_path / "a" / f).read_bytes() == (tmp_path / "b" / f).read_bytes(), f


def test_different_seed_different_output(tmp_path):
    args = dict(vehicles=15, hours=4, start="2026-10-01T00:00:00.000Z")
    a = gen.generate(seed=1, out=str(tmp_path / "a"), **args)
    b = gen.generate(seed=2, out=str(tmp_path / "b"), **args)
    for t in INPUT_TOPICS:
        assert a["topics"][t]["sha256"] != b["topics"][t]["sha256"], t


def test_committed_sample_is_reproducible(sample):
    committed = ROOT / "fixtures/sample"
    for f in ["MANIFEST.json", *(f"{t}.jsonl" for t in INPUT_TOPICS)]:
        assert (committed / f).read_bytes() == (sample.out / f).read_bytes(), (
            f"{f} differs; regenerate with: python generator/gen.py --seed 42 --vehicles 20 --hours 6 "
            "--out fixtures/sample")


def test_generator_uses_stdlib_only_and_no_clock_or_network():
    src = (ROOT / "generator/gen.py").read_text()
    tree = ast.parse(src)
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            modules.add(node.module.split(".")[0])
    assert modules <= set(sys.stdlib_module_names), modules
    assert not modules & {"time", "socket", "urllib", "http", "secrets", "os", "subprocess"}, modules
    attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    assert not attrs & {"now", "utcnow", "today", "time", "uuid1", "uuid4", "urandom", "SystemRandom"}
    # only seeded random.Random instances; no module-level random.* calls
    for n in ast.walk(tree):
        if isinstance(n, ast.Attribute) and isinstance(n.value, ast.Name) and n.value.id == "random":
            assert n.attr == "Random", n.attr


# ------------------------------------------------------------------ envelope

def test_vins(run):
    vins = {r["vin"] for t in INPUT_TOPICS for r in run.records[t]}
    assert len(vins) == run.manifest["arguments"]["vehicles"]
    assert all(VIN_RE.match(v) and v.startswith("SYN") for v in vins)
    for t in INPUT_TOPICS:
        assert {r["vin"] for r in run.records[t]} == vins, t


def test_vehicle_attributes_are_stable(run):
    attrs = {}
    for t in INPUT_TOPICS:
        for r in run.records[t]:
            a = (r["vehicle_model"], r["fleet"], r["market"])
            assert attrs.setdefault(r["vin"], a) == a


def test_producer_seq_follows_event_time(run):
    for t in INPUT_TOPICS:
        seen = set()
        uniq = [r for r in run.records[t] if not (r["event_id"] in seen or seen.add(r["event_id"]))]
        for vin, rs in by_vin(uniq).items():
            rs.sort(key=lambda r: r["event_time"])
            assert [r["producer_seq"] for r in rs] == list(range(1, len(rs) + 1)), (t, vin)
            times = [r["event_time"] for r in rs]
            assert len(set(times)) == len(times), (t, vin)


def test_event_ids_unique_except_duplicates(run):
    for t in INPUT_TOPICS:
        payload = {}
        for line, r in zip(run.lines[t], run.records[t]):
            assert payload.setdefault(r["event_id"], line) == line, t


def test_fleet_mix(run):
    fleets = {}
    for r in run.records["vehicle.battery.health.v1"]:
        fleets[r["vin"]] = (r["fleet"], r["market"])
    n = len(fleets)
    robotaxis = [m for f, m in fleets.values() if f == "robotaxi"]
    assert len(robotaxis) == gen.quota(n, 0.30)
    assert set(robotaxis) <= {"us-bay-area", "us-houston"}
    for r in run.events("vehicle.charging.events.v1"):
        if r["fleet"] == "robotaxi":
            assert r["charger_type"] == "dc_fast"


# ------------------------------------------------------------------ anomalies

def rates(runs_records, key):
    num = den = 0
    for m in runs_records:
        for t in INPUT_TOPICS:
            num += m["topics"][t][key]
            den += m["topics"][t]["unique_non_heartbeat_events"]
    return num / den


def test_duplicate_rate(run):
    assert 0.015 <= rates([run.manifest], "duplicates") <= 0.025
    for t in INPUT_TOPICS:
        if run.manifest["topics"][t]["unique_non_heartbeat_events"] >= 200:
            m = run.manifest["topics"][t]
            assert 0.015 <= m["duplicates"] / m["unique_non_heartbeat_events"] <= 0.025, t


def test_duplicates_are_byte_identical_and_follow_original(run):
    total = 0
    for t in INPUT_TOPICS:
        first_line = {}
        for i, (line, r) in enumerate(zip(run.lines[t], run.records[t])):
            if r["event_id"] in first_line:
                j, original = first_line[r["event_id"]]
                assert line == original and j < i
                assert r["event_type"] != "heartbeat"
                total += 1
            else:
                first_line[r["event_id"]] = (i, line)
    assert total == run.manifest["totals"]["duplicates"]


def observed_lateness(records):
    """(late events, max lateness ms) in arrival order, topic-wide, ignoring duplicate copies."""
    seen, high, late, worst = set(), None, 0, 0
    for r in records:
        if r["event_id"] in seen:
            continue
        seen.add(r["event_id"])
        t = ms(r["event_time"])
        if high is not None and high > t:
            late += 1
            worst = max(worst, high - t)
            assert r["event_type"] != "heartbeat"
        high = t if high is None else max(high, t)
    return late, worst


def test_late_rate_and_max_lateness(run):
    assert 0.02 <= rates([run.manifest], "late") <= 0.04
    for t in INPUT_TOPICS:
        late, worst = observed_lateness(run.records[t])
        m = run.manifest["topics"][t]
        assert late == m["late"], t
        assert worst <= 300_000 and worst / 1000 == m["max_lateness_seconds"], t
        if m["unique_non_heartbeat_events"] >= 200:
            assert 0.02 <= late / m["unique_non_heartbeat_events"] <= 0.04, t
    assert run.manifest["totals"]["late_same_vin"] > 0


def test_heartbeats(run):
    window_end = ms(run.manifest["window"]["end"])
    for t in INPUT_TOPICS:
        for vin, rs in by_vin(run.records[t]).items():
            hbs = [r for r in rs if r["event_type"] == "heartbeat"]
            assert len(hbs) == 1, (t, vin)
            assert rs[-1] is hbs[0], (t, vin)
            events = [r for r in rs if r["event_type"] != "heartbeat"]
            last = max((ms(r["event_time"]) for r in events), default=window_end)
            assert ms(hbs[0]["event_time"]) - last == TWO_HOURS_MS, (t, vin)
            assert hbs[0]["producer_seq"] == len({r["event_id"] for r in events}) + 1


# ------------------------------------------------------------------ geography

def test_coordinates_near_market_center(run):
    pairs = {"vehicle.charging.events.v1": ("charger_lat", "charger_lon"),
             "vehicle.diagnostics.dtc.v1": ("lat", "lon")}
    checked = 0
    for t, (la, lo) in pairs.items():
        for r in run.events(t):
            d = km((r[la], r[lo]), gen.MARKETS[r["market"]])
            assert d <= 40.0, (t, r["event_id"], d)
            assert decimals_of(r[la]) == 5 and decimals_of(r[lo]) == 5
            checked += 1
    assert checked > 0


def test_home_charging_is_within_home_radius(run):
    for r in run.events("vehicle.charging.events.v1"):
        if r["charger_type"] == "home":
            assert km((r["charger_lat"], r["charger_lon"]), gen.MARKETS[r["market"]]) <= 25.01


# ------------------------------------------------------------------ physical consistency

def sessions(run):
    out = []
    for vin, rs in by_vin(run.events("vehicle.charging.events.v1")).items():
        rs.sort(key=lambda r: r["event_time"])
        for r in rs:
            if r["event_type"] == "charge_start":
                out.append([r])
            else:
                out[-1].append(r)
    return out


def test_charging_sessions_are_well_formed(run):
    ss = sessions(run)
    assert ss
    no_stop = 0
    for s in ss:
        assert s[0]["event_type"] == "charge_start"
        assert all(r["event_type"] == "charge_progress" for r in s[1:-1])
        no_stop += s[-1]["event_type"] != "charge_stop"
        for a, b in zip(s, s[1:]):
            gap = ms(b["event_time"]) - ms(a["event_time"])
            assert gap < 30 * 60 * 1000
            for f in ("connector_type", "charger_type", "charger_lat", "charger_lon"):
                assert a[f] == b[f]
    assert no_stop == gen.quota(len(ss), 0.03, minimum=1)


def test_sessions_of_a_vin_are_45_minutes_apart(tmp_path):
    # the default-size run, where a 40-minute pad produced gaps under 45 minutes
    out = tmp_path / "default"
    run = Run(out, gen.generate(out=str(out)))
    by_vin = defaultdict(list)
    for s in sessions(run):
        by_vin[s[0]["vin"]].append((ms(s[0]["event_time"]), ms(s[-1]["event_time"])))
    assert any(len(spans) > 1 for spans in by_vin.values())
    for spans in by_vin.values():
        spans.sort()
        for (_, end), (start, _) in zip(spans, spans[1:]):
            assert start - end >= gen.MIN_SESSION_GAP_MS


def test_energy_never_decreases(run):
    for s in sessions(run):
        energy = [r["energy_added_kwh"] for r in s]
        soc = [r["soc_pct"] for r in s]
        assert energy[0] == 0 and energy == sorted(energy)
        assert soc == sorted(soc)


def test_power_within_charger_cap(run):
    for r in run.events("vehicle.charging.events.v1"):
        assert 0 <= r["power_kw"] <= gen.POWER_CAPS_KW[r["charger_type"]]
        if r["event_type"] != "charge_progress":
            assert r["power_kw"] == 0


def test_cell_voltage_delta_formula(run):
    for r in run.events("vehicle.battery.health.v1"):
        diff = (Decimal(repr(r["cell_voltage_max_v"])) - Decimal(repr(r["cell_voltage_min_v"]))) * 1000
        assert r["cell_voltage_delta_mv"] == int(diff.quantize(Decimal(1), rounding=ROUND_HALF_UP))
        assert r["cell_voltage_min_v"] <= r["cell_voltage_max_v"]


def test_battery_samples_every_ten_minutes(run):
    for vin, rs in by_vin(run.events("vehicle.battery.health.v1")).items():
        times = sorted(ms(r["event_time"]) for r in rs)
        for a, b in zip(times, times[1:]):
            assert b - a >= 9 * 60 * 1000


def test_warranty_signal(run):
    warranty = set(run.manifest["warranty_signal_vins"])
    n = run.manifest["arguments"]["vehicles"]
    assert len(warranty) == gen.quota(n, 0.01, minimum=1)
    for vin, rs in by_vin(run.events("vehicle.battery.health.v1")).items():
        rs.sort(key=lambda r: r["event_time"])
        deltas = [r["cell_voltage_delta_mv"] for r in rs]
        if vin in warranty:
            assert deltas == sorted(deltas) and deltas[-1] > 60 and deltas[0] <= 40
        else:
            assert all(5 <= d <= 40 for d in deltas)


def test_failure_code_iff_failed(run):
    for r in run.events("vehicle.ota.status.v1"):
        assert (r["failure_code"] is not None) == (r["state"] == "failed")


def test_ota_rollout(run):
    events = run.events("vehicle.ota.status.v1")
    vins = by_vin(events)
    n = run.manifest["arguments"]["vehicles"]
    assert len(vins) == gen.quota(n, 0.60)
    finals = Counter()
    for vin, rs in vins.items():
        rs.sort(key=lambda r: r["event_time"])
        assert rs[0]["state"] == "downloaded" and len(rs) <= 2
        finals[rs[-1]["state"]] += 1
        assert all(r["campaign_id"] == "OTA-2026.10.1" and r["target_version"] == "2026.10.1" for r in rs)
    assert finals["failed"] >= 1 and finals["installed"] > finals["failed"]


def test_firmware_switches_after_install(run):
    installed = {r["vin"]: r for r in run.events("vehicle.ota.status.v1") if r["state"] == "installed"}
    assert installed
    for t in INPUT_TOPICS:
        for r in run.records[t]:
            if r["vin"] in installed:
                after = r["event_time"] >= installed[r["vin"]]["event_time"]
                expected = "2026.10.1" if after else installed[r["vin"]]["from_version"]
                assert r["firmware_version"] == expected, (t, r["event_id"])


def test_dtc_mix(larger):
    events = larger.events("vehicle.diagnostics.dtc.v1")
    active = [r for r in events if r["status"] == "active"]
    assert len(active) == gen.quota(100, 0.5)
    assert any(r["status"] == "cleared" for r in events)
    sev = Counter(r["severity"] for r in active)
    # exact largest-remainder split of 40/30/18/9/3 percent
    assert sev == Counter({1: 20, 2: 15, 3: 9, 4: 5, 5: 1})


def test_sample_has_a_routable_alert(sample):
    assert any(r["severity"] >= 3 and r["status"] == "active" for r in sample.events("vehicle.diagnostics.dtc.v1"))


# ------------------------------------------------------------------ manifest

def test_manifest_matches_files(run):
    m = run.manifest
    assert m == json.loads((run.out / "MANIFEST.json").read_text())
    assert m["arguments"]["seed"] == SAMPLE_ARGS["seed"]
    total = 0
    for t in INPUT_TOPICS:
        mt = m["topics"][t]
        data = (run.out / mt["file"]).read_bytes()
        assert hashlib.sha256(data).hexdigest() == mt["sha256"]
        recs = run.records[t]
        assert mt["records"] == len(recs)
        assert mt["records_by_event_type"] == {et: sum(r["event_type"] == et for r in recs)
                                               for et in gen.EVENT_TYPES[t]}
        assert mt["unique_non_heartbeat_events"] == len(run.events(t))
        total += len(recs)
    assert m["totals"]["records"] == total


def test_cli_defaults(monkeypatch):
    seen = {}
    monkeypatch.setattr(gen, "generate", lambda *a: seen.setdefault("args", a) and {"totals": {}})
    gen.main([])
    assert seen["args"] == (42, 500, 24, "2026-10-01T00:00:00.000Z", "fixtures/")
