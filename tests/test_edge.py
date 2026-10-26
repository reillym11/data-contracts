import hashlib
import json
from collections import Counter

import pytest

from conftest import INPUT_TOPICS, ROOT

EDGE = ROOT / "fixtures/edge"
MANIFEST = json.loads((EDGE / "MANIFEST.json").read_text())


def lines(topic):
    return (EDGE / f"{topic}.jsonl").read_text().splitlines()


@pytest.mark.parametrize("topic", INPUT_TOPICS)
def test_every_edge_record_validates(validators, topic):
    for n, line in enumerate(lines(topic), 1):
        errs = [e.message for e in validators[topic].iter_errors(json.loads(line))]
        assert not errs, (topic, n, errs)


@pytest.mark.parametrize("topic", INPUT_TOPICS)
def test_edge_manifest_matches_files(topic):
    m = MANIFEST["topics"][topic]
    records = [json.loads(line) for line in lines(topic)]
    assert m["sha256"] == hashlib.sha256((EDGE / m["file"]).read_bytes()).hexdigest()
    assert m["records"] == len(records)
    assert m["records_by_event_type"] == dict(Counter(r["event_type"] for r in records))
    assert {r["vin"] for r in records} == set(MANIFEST["vins"])
