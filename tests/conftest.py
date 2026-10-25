import json
import sys
from pathlib import Path

import pytest
import yaml
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "generator"))

import gen  # noqa: E402

SAMPLE_ARGS = {"seed": 42, "vehicles": 20, "hours": 6, "start": "2026-10-01T00:00:00.000Z"}
INPUT_TOPICS = gen.TOPICS
PRIVACY_TAGS = {"none", "vehicle-id", "precise-location", "coarse-location"}


def load_schemas():
    common = json.loads((ROOT / "contracts/common.schema.json").read_text())
    topics = {}
    for path in sorted((ROOT / "contracts/topics").glob("*.schema.json")):
        topics[path.name[: -len(".schema.json")]] = json.loads(path.read_text())
    return common, topics


@pytest.fixture(scope="session")
def schemas():
    return load_schemas()


@pytest.fixture(scope="session")
def topic_schemas(schemas):
    return schemas[1]


@pytest.fixture(scope="session")
def validators(schemas):
    common, topics = schemas
    registry = Registry().with_resources(
        (s["$id"], Resource.from_contents(s)) for s in [common, *topics.values()]
    )
    return {name: Draft202012Validator(s, registry=registry) for name, s in topics.items()}


@pytest.fixture(scope="session")
def registry_yaml():
    return yaml.safe_load((ROOT / "contracts/topics.yaml").read_text())


class Run:
    """A generated fixture set: raw lines and parsed records per input topic."""

    def __init__(self, out: Path, manifest: dict):
        self.out = out
        self.manifest = manifest
        self.lines = {t: (out / f"{t}.jsonl").read_text().splitlines() for t in INPUT_TOPICS}
        self.records = {t: [json.loads(line) for line in ls] for t, ls in self.lines.items()}

    def events(self, topic):
        """Unique non-heartbeat records in arrival order."""
        seen, out = set(), []
        for r in self.records[topic]:
            if r["event_type"] == "heartbeat" or r["event_id"] in seen:
                continue
            seen.add(r["event_id"])
            out.append(r)
        return out


@pytest.fixture(scope="session")
def sample(tmp_path_factory):
    out = tmp_path_factory.mktemp("sample")
    return Run(out, gen.generate(out=str(out), **SAMPLE_ARGS))


@pytest.fixture(scope="session")
def larger(tmp_path_factory):
    """A bigger run so rate checks are not dominated by rounding."""
    out = tmp_path_factory.mktemp("larger")
    args = dict(SAMPLE_ARGS, vehicles=100, hours=24)
    return Run(out, gen.generate(out=str(out), **args))


@pytest.fixture(params=["sample", "larger"])
def run(request):
    return request.getfixturevalue(request.param)
