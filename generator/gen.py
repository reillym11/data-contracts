#!/usr/bin/env python3
"""Deterministic synthetic telemetry generator for the data-contracts project.

Python standard library only. Never reads the wall clock, never touches the network.
All randomness comes from random.Random(seed). The same arguments produce byte-identical files.

Mock project for a demo. All data is synthetic.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import random
import re
import uuid
from pathlib import Path

GENERATOR_VERSION = "1"

TOPICS = [
    "vehicle.charging.events.v1",
    "vehicle.ota.status.v1",
    "vehicle.battery.health.v1",
    "vehicle.diagnostics.dtc.v1",
]
CHARGING, OTA, BATTERY, DTC = TOPICS
EVENT_TYPES = {
    CHARGING: ["charge_start", "charge_progress", "charge_stop", "heartbeat"],
    OTA: ["ota_status", "heartbeat"],
    BATTERY: ["battery_sample", "heartbeat"],
    DTC: ["dtc", "heartbeat"],
}
DOMAIN_FIELDS = {
    CHARGING: ["connector_type", "charger_type", "power_kw", "energy_added_kwh", "soc_pct",
               "charger_lat", "charger_lon"],
    OTA: ["campaign_id", "from_version", "target_version", "state", "failure_code"],
    BATTERY: ["soh_pct", "cell_voltage_min_v", "cell_voltage_max_v", "cell_voltage_delta_mv",
              "pack_temp_c", "soc_pct", "odometer_km"],
    DTC: ["dtc_code", "severity", "ecu", "status", "lat", "lon", "odometer_km"],
}

MARKETS = {
    "us-bay-area": (37.55, -122.05),
    "us-houston": (29.76, -95.37),
    "us-phoenix": (33.45, -112.07),
}
ROBOTAXI_MARKETS = ["us-bay-area", "us-houston"]
VIN_ALPHABET = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
PACKS = {"AIR": [84, 92, 118], "GRAVITY": [92, 118]}
BASE_FIRMWARE = ["2026.8.3", "2026.9.2"]
CAMPAIGN_ID = "OTA-2026.10.1"
TARGET_VERSION = "2026.10.1"
FAILURE_CODES = ["E_DOWNLOAD_CORRUPT", "E_INSTALL_PRECHECK", "E_BATTERY_LOW", "E_TIMEOUT"]
ECUS_BY_SYSTEM = {
    "P": ["inverter_front", "inverter_rear", "bms"],
    "B": ["zone_left", "zone_right"],
    "C": ["zone_left", "zone_right", "central_compute"],
    "U": ["central_compute"],
}
SEVERITY_WEIGHTS = [(1, 40), (2, 30), (3, 18), (4, 9), (5, 3)]
POWER_CAPS_KW = {"home": 11.5, "ac_l2": 19.2, "dc_fast": 400.0}

HOME_RADIUS_KM = 25.0
SITE_RADIUS_KM = 30.0
SITES_PER_MARKET = 40
DC_SITES_PER_MARKET = 15
GAP_TIMEOUT_MS = 30 * 60 * 1000
MIN_SESSION_GAP_MS = 45 * 60 * 1000
HEARTBEAT_OFFSET_MS = 2 * 60 * 60 * 1000
MAX_LATENESS_MS = 300 * 1000
DUP_RATE = 0.02
LATE_RATE = 0.03
NO_STOP_RATE = 0.03
ROBOTAXI_SHARE = 0.30
WARRANTY_SHARE = 0.01
OTA_SHARE = 0.60

MIN = 60 * 1000
TS_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{3})Z$")
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


# ---------------------------------------------------------------- helpers

def parse_ts(s: str) -> int:
    m = TS_RE.match(s)
    if not m:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DDTHH:MM:SS.mmmZ, got {s!r}")
    y, mo, d, h, mi, sec, ms = (int(x) for x in m.groups())
    t = dt.datetime(y, mo, d, h, mi, sec, tzinfo=dt.timezone.utc)
    return (t - EPOCH) // dt.timedelta(milliseconds=1) + ms


def fmt_ts(ms: int) -> str:
    t = EPOCH + dt.timedelta(milliseconds=ms)
    return t.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"


def q(x: float, decimals: int) -> float:
    """Quantize to a fixed number of decimals; the result prints with at most that many."""
    scale = 10 ** decimals
    return int(round(x * scale)) / scale


def coord(x: float) -> float:
    """5-decimal coordinate whose 5th decimal is never 0, so it always prints exactly 5 decimals."""
    units = int(round(x * 100000))
    if units % 10 == 0:
        units += 1
    return units / 100000


def point_near(rng: random.Random, center: tuple[float, float], radius_km: float) -> tuple[float, float]:
    r = radius_km * math.sqrt(rng.random())
    theta = rng.random() * 2 * math.pi
    lat = center[0] + (r * math.cos(theta)) / 111.32
    lon = center[1] + (r * math.sin(theta)) / (111.32 * math.cos(math.radians(center[0])))
    return coord(lat), coord(lon)


def new_uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def quota(n: int, share: float, minimum: int = 0) -> int:
    return min(n, max(minimum, int(math.floor(n * share + 0.5))))


def stochastic_round(rng: random.Random, x: float) -> int:
    base = int(math.floor(x))
    return base + (1 if rng.random() < x - base else 0)


def apportion(n: int, pairs):
    """Exactly n values split by weight (largest remainder; ties go to the earlier value)."""
    total = sum(w for _, w in pairs)
    shares = [(value, n * w / total) for value, w in pairs]
    counts = {value: int(math.floor(x)) for value, x in shares}
    order = sorted(range(len(shares)), key=lambda i: (-(shares[i][1] - math.floor(shares[i][1])), i))
    for i in order[: n - sum(counts.values())]:
        counts[shares[i][0]] += 1
    return [value for value, _ in pairs for _ in range(counts[value])]


def weighted(rng: random.Random, pairs):
    total = sum(w for _, w in pairs)
    x = rng.random() * total
    for value, w in pairs:
        x -= w
        if x < 0:
            return value
    return pairs[-1][0]


def dumps(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))


# ---------------------------------------------------------------- population

class Vehicle:
    def __init__(self, idx, vin, fleet, market, model, pack_kwh, home, firmware, soh, delta_base, odometer, soc):
        self.idx = idx
        self.vin = vin
        self.fleet = fleet
        self.market = market
        self.model = model
        self.pack_kwh = pack_kwh
        self.home = home
        self.firmware = firmware
        self.soh = soh
        self.delta_base = delta_base
        self.odometer0 = odometer
        self.soc0 = soc
        self.segments = []      # (kind, start_ms, end_ms, info) sorted by start
        self.knots = []         # (t_ms, soc, odometer_km) piecewise-linear state
        self.charge_events = [] # domain dicts with "t" and "event_type"
        self.install_ms = None  # firmware switches to TARGET_VERSION at this time
        self.warranty = False
        self.delta_final = None


def make_population(rng: random.Random, n: int):
    vins, seen = [], set()
    while len(vins) < n:
        v = "SYN" + "".join(rng.choice(VIN_ALPHABET) for _ in range(14))
        if v not in seen:
            seen.add(v)
            vins.append(v)
    robotaxi = set(rng.sample(range(n), quota(n, ROBOTAXI_SHARE)))
    market_names = list(MARKETS)
    vehicles = []
    for i, vin in enumerate(vins):
        fleet = "robotaxi" if i in robotaxi else "consumer"
        market = rng.choice(ROBOTAXI_MARKETS if fleet == "robotaxi" else market_names)
        model = rng.choice(["AIR", "GRAVITY"])
        vehicles.append(Vehicle(
            idx=i, vin=vin, fleet=fleet, market=market, model=model,
            pack_kwh=rng.choice(PACKS[model]),
            home=point_near(rng, MARKETS[market], HOME_RADIUS_KM),
            firmware=rng.choice(BASE_FIRMWARE),
            soh=rng.uniform(88.5, 99.8),
            delta_base=rng.randint(8, 34),
            odometer=rng.randint(50000, 220000) if fleet == "robotaxi" else rng.randint(2000, 80000),
            soc=rng.uniform(30, 70) if fleet == "robotaxi" else rng.uniform(40, 85),
        ))
    sites = {}
    for m in market_names:
        pool = []
        for k in range(SITES_PER_MARKET):
            lat, lon = point_near(rng, MARKETS[m], SITE_RADIUS_KM)
            pool.append({"lat": lat, "lon": lon, "type": "dc_fast" if k < DC_SITES_PER_MARKET else "ac_l2"})
        sites[m] = pool
    return vehicles, sites


# ---------------------------------------------------------------- activity timeline

def free(busy, start, end, pad):
    return all(end + pad <= b0 or start >= b1 + pad for b0, b1 in busy)


def place(rng, busy, t0, t1, dur, pad, tries=60):
    if t1 - t0 <= dur:
        return None
    for _ in range(tries):
        s = rng.randint(t0, t1 - dur)
        if free(busy, s, s + dur, pad):
            return s
    return None


def plan_vehicle(rng: random.Random, v: Vehicle, sites, t0: int, t1: int, hours: float):
    """Lay out non-overlapping charging slots and driving trips inside [t0, t1)."""
    days = hours / 24.0
    edge = 5 * MIN
    charge_slots = []  # (start, end, charger_type)
    busy = []
    if v.fleet == "robotaxi":
        n_sessions = stochastic_round(rng, rng.uniform(2, 4) * days)
        kinds = ["dc_fast"] * n_sessions
    else:
        n_sessions = sum(1 for _ in range(max(1, math.ceil(2 * days))) if rng.random() < 0.45)
        kinds = [weighted(rng, [("home", 65), ("ac_l2", 25), ("dc_fast", 10)]) for _ in range(n_sessions)]
    for kind in kinds:
        dur = rng.randint(20, 45) * MIN if kind == "dc_fast" else rng.randint(60, 240) * MIN
        # sessions are separated by more than the gap timeout plus max lateness, so a missing charge_stop
        # closes by gap and a late event is never within the gap timeout of a neighboring session
        s = place(rng, busy, t0 + edge, t1 - edge, dur, MIN_SESSION_GAP_MS)
        if s is None:
            continue
        busy.append((s, s + dur))
        charge_slots.append((s, s + dur, kind))
    trips = []
    if v.fleet == "robotaxi":
        t = t0 + rng.randint(0, 20) * MIN
        while t < t1 - 30 * MIN:
            dur = rng.randint(30, 120) * MIN
            end = min(t + dur, t1 - edge)
            # skip forward past any charging slot this trip would overlap
            clash = [b for b in busy if not (end + 2 * MIN <= b[0] or t >= b[1] + 2 * MIN)]
            if clash:
                t = max(b[1] for b in clash) + rng.randint(3, 10) * MIN
                continue
            if end - t >= 10 * MIN:
                trips.append((t, end))
            t = end + rng.randint(5, 20) * MIN
    else:
        for _ in range(stochastic_round(rng, 2.5 * days)):
            dur = rng.randint(15, 60) * MIN
            s = place(rng, busy, t0 + edge, t1 - edge, dur, 5 * MIN)
            if s is None:
                continue
            busy.append((s, s + dur))
            trips.append((s, s + dur))
    segs = [("charge", s, e, k) for s, e, k in charge_slots] + [("drive", s, e, None) for s, e in trips]
    segs.sort(key=lambda x: x[1])
    return segs


def simulate_vehicle(rng: random.Random, v: Vehicle, sites, segs):
    soc, odo = v.soc0, float(v.odometer0)
    knots = [(segs[0][1] if segs else 0, soc, odo)] if segs else []
    out_segments = []
    for kind, s, e, info in segs:
        knots.append((s, soc, odo))
        if kind == "drive":
            kw = rng.uniform(6.0, 11.0) if v.fleet == "consumer" else rng.uniform(7.0, 12.0)
            kmh = rng.uniform(30, 55)
            minutes = (e - s) / MIN
            usable_min = max(0.0, (soc - 10.0) / 100 * v.pack_kwh / kw * 60)
            minutes = min(minutes, usable_min)
            if minutes < 5:
                continue
            e = s + int(minutes * MIN)
            soc -= kw * minutes / 60 / v.pack_kwh * 100
            odo += kmh * minutes / 60
            knots.append((e, soc, odo))
            out_segments.append(("drive", s, e, None))
        else:
            events, soc = simulate_charge(rng, v, sites, s, e, info, soc)
            for ev in events:
                knots.append((ev["t"], ev["soc_pct"], odo))
            v.charge_events.append(events)
            out_segments.append(("charge", s, events[-1]["t"], events[0]["charger_type"]))
    v.segments = out_segments
    v.knots = sorted(knots, key=lambda k: k[0])


def simulate_charge(rng, v, sites, s, e, charger_type, soc):
    if charger_type == "home":
        lat, lon = v.home
    else:
        pool = [site for site in sites[v.market] if site["type"] == charger_type]
        site = rng.choice(pool)
        lat, lon = site["lat"], site["lon"]
    if charger_type == "dc_fast":
        connector = rng.choice(["NACS", "CCS1"])
        p_max = rng.uniform(150, 300) if v.fleet == "robotaxi" else rng.uniform(50, 250)
        target = rng.uniform(85, 95) if v.fleet == "robotaxi" else rng.uniform(80, 92)
        step = (60, 180)
    else:
        connector = rng.choice(["J1772", "NACS"])
        p_base = rng.uniform(7.2, 10.8)
        target = rng.uniform(80, 92)
        step = (120, 300)
    common = {"connector_type": connector, "charger_type": charger_type, "charger_lat": lat, "charger_lon": lon}
    t = s + rng.randint(0, 59999)
    energy = 0.0
    soc = q(soc, 1)
    events = [dict(common, t=t, event_type="charge_start", power_kw=0.0, energy_added_kwh=0.0, soc_pct=soc)]
    while True:
        nt = t + rng.randint(step[0] * 1000, step[1] * 1000)
        if nt >= e:
            break
        if charger_type == "dc_fast":
            taper = 1.0 if soc < 80 else max(0.2, 1 - (soc - 80) / 20 * 0.8)
            p = max(50.0, min(300.0, p_max * taper * rng.uniform(0.97, 1.0)))
        else:
            p = min(11.0, max(7.0, p_base + rng.uniform(-0.3, 0.3)))
        p = q(p, 1)
        added = p * (nt - t) / 3_600_000
        energy = q(energy + added, 3)
        soc = q(min(100.0, soc + added / v.pack_kwh * 100), 1)
        t = nt
        events.append(dict(common, t=t, event_type="charge_progress", power_kw=p, energy_added_kwh=energy, soc_pct=soc))
        if soc >= target:
            break
    t += rng.randint(5000, 30000)
    events.append(dict(common, t=t, event_type="charge_stop", power_kw=0.0, energy_added_kwh=energy, soc_pct=soc))
    return events, soc


def state_at(v: Vehicle, t: int):
    ks = v.knots
    if not ks or t <= ks[0][0]:
        return (ks[0][1], ks[0][2]) if ks else (v.soc0, float(v.odometer0))
    for (ta, sa, oa), (tb, sb, ob) in zip(ks, ks[1:]):
        if ta <= t <= tb:
            f = 0 if tb == ta else (t - ta) / (tb - ta)
            return sa + (sb - sa) * f, oa + (ob - oa) * f
    return ks[-1][1], ks[-1][2]


def firmware_at(v: Vehicle, t: int) -> str:
    return TARGET_VERSION if v.install_ms is not None and t >= v.install_ms else v.firmware


# ---------------------------------------------------------------- per-topic domain events

def battery_events(rng, v: Vehicle):
    """Sample times on one 10-minute grid per vehicle, kept while it is driving or charging."""
    if not v.segments:
        return []
    times = []
    k, end = v.segments[0][1], max(e for _, _, e, _ in v.segments)
    while k < end:
        seg = next((sg for sg in v.segments if sg[1] <= k < sg[2]), None)
        if seg is not None:
            times.append((k, seg[0], seg[3]))
        k += 10 * MIN
    return times


def build_battery(rng, vehicles, t0, t1):
    per_vehicle = {v.idx: battery_events(rng, v) for v in vehicles}
    eligible = [v.idx for v in vehicles if len(per_vehicle[v.idx]) >= 3]
    k = quota(len(vehicles), WARRANTY_SHARE, minimum=1)
    for idx in rng.sample(eligible, min(k, len(eligible))):
        vehicles[idx].warranty = True
        vehicles[idx].delta_final = rng.randint(62, 75)
    out = []
    for v in vehicles:
        times = per_vehicle[v.idx]
        if not times:
            continue
        first, last = times[0][0], times[-1][0]
        for t, kind, info in times:
            t_ev = t + rng.randint(0, 999)
            soc, odo = state_at(v, t_ev)
            if v.warranty:
                frac = 0 if last == first else (t - first) / (last - first)
                delta = int(round(v.delta_base + (v.delta_final - v.delta_base) * frac))
            else:
                delta = min(40, max(5, v.delta_base + rng.randint(-3, 3)))
            mean_mv = int(round((3300 + 9 * soc)))
            min_mv = mean_mv - delta // 2
            max_mv = min_mv + delta
            if kind == "drive":
                temp = rng.uniform(24, 36)
            elif info == "dc_fast":
                temp = rng.uniform(32, 45)
            else:
                temp = rng.uniform(20, 31)
            out.append({
                "vin": v.vin, "t": t_ev, "event_type": "battery_sample",
                "soh_pct": q(min(100.0, max(80.0, v.soh + rng.uniform(-0.02, 0.02))), 2),
                "cell_voltage_min_v": min_mv / 1000,
                "cell_voltage_max_v": max_mv / 1000,
                "cell_voltage_delta_mv": int(round((max_mv / 1000 - min_mv / 1000) * 1000)),
                "pack_temp_c": q(temp, 1),
                "soc_pct": q(soc, 1),
                "odometer_km": int(odo),
            })
    return out


def build_charging(rng, vehicles):
    sessions = [(v, evs) for v in vehicles for evs in v.charge_events]
    no_stop = set(rng.sample(range(len(sessions)), quota(len(sessions), NO_STOP_RATE, minimum=1 if sessions else 0)))
    out = []
    for i, (v, evs) in enumerate(sessions):
        for ev in evs:
            if i in no_stop and ev["event_type"] == "charge_stop":
                continue
            out.append(dict(ev, vin=v.vin))
    return out


def build_ota(rng, vehicles, t0, t1):
    rollout = rng.sample(range(len(vehicles)), quota(len(vehicles), OTA_SHARE))
    m = len(rollout)
    n_fail = quota(m, 0.05, minimum=1 if m >= 2 else 0)
    n_stuck = quota(m, 0.02, minimum=1 if m >= 3 else 0)
    outcomes = ["failed"] * n_fail + ["downloaded"] * n_stuck + ["installed"] * (m - n_fail - n_stuck)
    rng.shuffle(outcomes)
    span = t1 - t0
    latest_download = t1 - 100 * MIN if span > 120 * MIN else t0 + span // 2
    out = []
    for idx, outcome in zip(rollout, outcomes):
        v = vehicles[idx]
        td = rng.randint(t0 + MIN, max(t0 + MIN, latest_download))
        base = {"vin": v.vin, "event_type": "ota_status", "campaign_id": CAMPAIGN_ID,
                "from_version": v.firmware, "target_version": TARGET_VERSION}
        out.append(dict(base, t=td, state="downloaded", failure_code=None))
        if outcome == "downloaded":
            continue
        t2 = min(td + rng.randint(10, 90) * MIN + rng.randint(0, 59999), t1 - 1)
        if outcome == "installed":
            v.install_ms = t2
            out.append(dict(base, t=t2, state="installed", failure_code=None))
        else:
            out.append(dict(base, t=t2, state="failed", failure_code=rng.choice(FAILURE_CODES)))
    return out


def build_dtc(rng, vehicles, t0, t1, hours):
    # exact fleet total (0.5 per vehicle-day), assigned to random vehicles
    total = quota(len(vehicles), 0.5 * hours / 24)
    owners = sorted(rng.randrange(len(vehicles)) for _ in range(total))
    severities = apportion(total, SEVERITY_WEIGHTS)
    rng.shuffle(severities)
    out = []
    for idx, severity in zip(owners, severities):
        v = vehicles[idx]
        t = rng.randint(t0, t1 - 1)
        area = v.home if v.fleet == "consumer" else point_near(rng, MARKETS[v.market], HOME_RADIUS_KM)
        lat, lon = point_near(rng, area, 5.0)
        system = weighted(rng, [("P", 40), ("B", 20), ("C", 15), ("U", 25)])
        code = system + "".join(rng.choice("0123456789ABCDEF") for _ in range(4))
        ev = {"vin": v.vin, "event_type": "dtc", "dtc_code": code,
              "severity": severity, "ecu": rng.choice(ECUS_BY_SYSTEM[system]),
              "status": "active", "lat": lat, "lon": lon}
        out.append(dict(ev, t=t, odometer_km=int(state_at(v, t)[1])))
        if rng.random() < 0.35:
            tc = t + rng.randint(5, 120) * MIN + rng.randint(0, 59999)
            if tc < t1:
                out.append(dict(ev, t=tc, status="cleared", odometer_km=int(state_at(v, tc)[1])))
    return out


# ---------------------------------------------------------------- envelope, anomalies, output

def finalize_topic(rng, topic, domain_events, vehicles, t1):
    by_vin = {v.vin: v for v in vehicles}
    fields = DOMAIN_FIELDS[topic]
    domain_events.sort(key=lambda e: (e["vin"], e["t"]))
    # strictly increasing event_time per VIN keeps producer_seq unambiguous
    last_t = {}
    for e in domain_events:
        if e["vin"] in last_t and e["t"] <= last_t[e["vin"]]:
            e["t"] = last_t[e["vin"]] + 1
        last_t[e["vin"]] = e["t"]
    seq, records = {}, []
    for e in domain_events:
        v = by_vin[e["vin"]]
        seq[v.vin] = seq.get(v.vin, 0) + 1
        rec = envelope(rng, v, e["t"], e["event_type"], seq[v.vin])
        for f in fields:
            rec[f] = e[f]
        records.append(rec)
    heartbeats = []
    for v in vehicles:
        ht = (last_t[v.vin] if v.vin in last_t else t1) + HEARTBEAT_OFFSET_MS
        rec = envelope(rng, v, ht, "heartbeat", seq.get(v.vin, 0) + 1)
        for f in fields:
            rec[f] = None
        heartbeats.append((ht, rec))
    return records, heartbeats


def envelope(rng, v: Vehicle, t: int, event_type: str, seq: int):
    return {
        "event_id": new_uuid(rng),
        "vin": v.vin,
        "event_time": fmt_ts(t),
        "event_type": event_type,
        "producer_seq": seq,
        "schema_version": "1",
        "vehicle_model": v.model,
        "fleet": v.fleet,
        "market": v.market,
        "firmware_version": firmware_at(v, t),
        "_t": t,
    }


def apply_anomalies(rng, records, heartbeats):
    """Return records in arrival order plus anomaly stats.

    Clean arrival order is event-time order. Late events are delayed so at least one later event of the
    topic overtakes them, by at most MAX_LATENESS_MS. Duplicates are exact copies arriving 1-60 s after
    their original. Heartbeats are never delayed or duplicated.
    """
    clean = sorted(records, key=lambda r: (r["_t"], r["vin"], r["producer_seq"]))
    n = len(clean)
    eligible = []
    for i, r in enumerate(clean):
        j = i + 1
        while j < n and clean[j]["_t"] <= r["_t"]:
            j += 1
        if j < n and clean[j]["_t"] - r["_t"] < MAX_LATENESS_MS:
            eligible.append((i, clean[j]["_t"] - r["_t"]))
    late = rng.sample(eligible, min(len(eligible), quota(n, LATE_RATE)))
    arrival = [r["_t"] for r in clean]
    for i, gap in sorted(late):
        arrival[i] = clean[i]["_t"] + rng.randint(gap + 1, MAX_LATENESS_MS)
    dup_idx = sorted(rng.sample(range(n), quota(n, DUP_RATE)))
    stream = [(arrival[i], 0, i, clean[i]) for i in range(n)]
    for k, i in enumerate(dup_idx):
        stream.append((arrival[i] + rng.randint(1000, 60000), 1, n + k, clean[i]))
    for k, (ht, rec) in enumerate(heartbeats):
        stream.append((ht, 2, 2 * n + k, rec))
    stream.sort(key=lambda x: (x[0], x[1], x[2]))
    return [rec for *_, rec in stream], len(dup_idx)


def measure(lines_records):
    """Observed anomalies in arrival order (independent of how they were injected)."""
    seen_ids, max_seen, max_seen_vin = set(), None, {}
    late = late_same_vin = dups = 0
    max_late_ms = 0
    for r in lines_records:
        t, vin = r["_t"], r["vin"]
        if r["event_id"] in seen_ids:
            dups += 1
            continue
        seen_ids.add(r["event_id"])
        if max_seen is not None and max_seen > t:
            late += 1
            max_late_ms = max(max_late_ms, max_seen - t)
            if max_seen_vin.get(vin, t) > t:
                late_same_vin += 1
        max_seen = t if max_seen is None else max(max_seen, t)
        max_seen_vin[vin] = max(max_seen_vin.get(vin, t), t)
    return late, late_same_vin, dups, max_late_ms


def generate(seed=42, vehicles=500, hours=24, start="2026-10-01T00:00:00.000Z", out="fixtures/"):
    if vehicles < 1 or hours <= 0:
        raise ValueError("vehicles must be >= 1 and hours > 0")
    t0 = parse_ts(start)
    t1 = t0 + int(round(hours * 3_600_000))
    master = random.Random(seed)
    rngs = {name: random.Random(master.getrandbits(64)) for name in
            ["population", "plan", "ota", "battery", "dtc", "charging", "envelope", "anomalies"]}
    fleet, sites = make_population(rngs["population"], vehicles)
    for v in fleet:
        simulate_vehicle(rngs["plan"], v, sites, plan_vehicle(rngs["plan"], v, sites, t0, t1, hours))
    domain = {
        OTA: build_ota(rngs["ota"], fleet, t0, t1),  # first: sets install times used by firmware_version
        CHARGING: build_charging(rngs["charging"], fleet),
        BATTERY: build_battery(rngs["battery"], fleet, t0, t1),
        DTC: build_dtc(rngs["dtc"], fleet, t0, t1, hours),
    }
    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "generator_version": GENERATOR_VERSION,
        "schema_version": "1",
        "arguments": {"seed": seed, "vehicles": vehicles, "hours": hours, "start": start},
        "window": {"start": fmt_ts(t0), "end": fmt_ts(t1)},
        "warranty_signal_vins": sorted(v.vin for v in fleet if v.warranty),
        "topics": {},
    }
    totals = {"records": 0, "duplicates": 0, "late": 0, "late_same_vin": 0, "max_lateness_seconds": 0.0}
    for topic in TOPICS:
        records, heartbeats = finalize_topic(rngs["envelope"], topic, domain[topic], fleet, t1)
        ordered, injected_dups = apply_anomalies(rngs["anomalies"], records, heartbeats)
        late, late_same_vin, dups, max_late_ms = measure(ordered)
        assert dups == injected_dups
        by_type = {et: 0 for et in EVENT_TYPES[topic]}
        lines = []
        for r in ordered:
            by_type[r["event_type"]] += 1
            lines.append(dumps({k: val for k, val in r.items() if k != "_t"}))
        data = ("\n".join(lines) + "\n").encode("utf-8")
        path = out_dir / f"{topic}.jsonl"
        path.write_bytes(data)
        non_hb = len(ordered) - by_type["heartbeat"] - dups
        manifest["topics"][topic] = {
            "file": path.name,
            "sha256": hashlib.sha256(data).hexdigest(),
            "records": len(ordered),
            "records_by_event_type": by_type,
            "unique_non_heartbeat_events": non_hb,
            "duplicates": dups,
            "late": late,
            "late_same_vin": late_same_vin,
            "max_lateness_seconds": max_late_ms / 1000,
        }
        totals["records"] += len(ordered)
        totals["duplicates"] += dups
        totals["late"] += late
        totals["late_same_vin"] += late_same_vin
        totals["max_lateness_seconds"] = max(totals["max_lateness_seconds"], max_late_ms / 1000)
    manifest["totals"] = totals
    (out_dir / "MANIFEST.json").write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return manifest


def main(argv=None):
    p = argparse.ArgumentParser(description="Deterministic synthetic vehicle telemetry generator (demo, synthetic data).")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--vehicles", type=int, default=500)
    p.add_argument("--hours", type=int, default=24)
    p.add_argument("--start", default="2026-10-01T00:00:00.000Z")
    p.add_argument("--out", default="fixtures/")
    a = p.parse_args(argv)
    parse_ts(a.start)
    m = generate(a.seed, a.vehicles, a.hours, a.start, a.out)
    print(json.dumps(m["totals"], sort_keys=True))


if __name__ == "__main__":
    main()
