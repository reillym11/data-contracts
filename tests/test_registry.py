from conftest import ROOT


def test_registry_covers_every_schema(registry_yaml, topic_schemas):
    names = [e["name"] for e in registry_yaml["topics"]]
    assert sorted(names) == sorted(topic_schemas)
    assert len(names) == len(set(names))


def test_registry_matches_schema_annotations(registry_yaml, topic_schemas):
    for e in registry_yaml["topics"]:
        s = topic_schemas[e["name"]]
        assert (ROOT / e["schema"]).is_file()
        assert e["schema"] == f"contracts/topics/{e['name']}.schema.json"
        assert e["key"] == s["x-key"]
        assert e["partitions"] == s["x-partitions"]
        assert e["cleanup_policy"] == s["x-cleanup-policy"]
        assert e["partitioner"] == s["x-partitioner"]
        assert e["producer"] == s["x-producer"]
        assert e["consumers"] == s["x-consumers"]
        if e["kind"] == "parquet-sink":
            assert e["sink"] == s["x-sink"] and e["partition_by"] == s["x-partition-by"]
            assert e["format"] == s["x-format"] == "parquet"
        else:
            assert e["kind"] == "kafka-topic"
            assert e["partitions"] == 6
            for field in e["key"].split("|"):
                assert field in s["properties"], (e["name"], field)
        assert e["direction"] == ("input" if s["x-producer"] == "vehicle" else "output")


def test_sensitive_fields_match_privacy_tags(registry_yaml, topic_schemas):
    for e in registry_yaml["topics"]:
        props = topic_schemas[e["name"]]["properties"]
        expected = [{"field": f, "privacy": p["x-privacy"]} for f, p in props.items() if p["x-privacy"] != "none"]
        assert e["sensitive_fields"] == expected, e["name"]


def test_consumers_of_inputs_produce_outputs(registry_yaml):
    entries = registry_yaml["topics"]
    producers = {e["producer"] for e in entries if e["direction"] == "output"}
    for e in entries:
        if e["direction"] == "input":
            assert set(e["consumers"]) <= producers, e["name"]
