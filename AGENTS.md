# AGENTS.md

Rules for anyone (human or agent) changing this repo or building on it.

**Mock project for a demo. All data is synthetic.** Never add real VINs, people, addresses or company code.

## Commands
- Install: `python -m pip install -r requirements-dev.txt` (Python 3.11)
- Test: `python -m pytest -q` (whole suite must stay under 60 seconds)
- Regenerate the committed sample: `python generator/gen.py --seed 42 --vehicles 20 --hours 6 --out fixtures/sample`

## Rules
1. `contracts/` is the contract. Changing a field, enum, key, partition count or derivation rule is a breaking
   change for the Spark, Rust and parity projects: add a new `.v2` topic instead of editing a `.v1` one.
2. Every property needs exactly one `x-privacy` tag (`none`, `vehicle-id`, `precise-location`, `coarse-location`).
   Lat/lon also need `x-precision` (5 precise, 2 coarse). Numbers need `x-decimals`.
3. Keep `contracts/topics.yaml` in sync with the schema `x-*` annotations; `tests/test_registry.py` enforces it.
4. Schema files are written with `json.dumps(obj, indent=2) + "\n"`; a test checks this.
5. `generator/gen.py` is stdlib only, seeded only through `random.Random(seed)`, and must never read the clock or
   the network. If you change it, regenerate `fixtures/sample/` in the same commit.
6. Output fields come from inputs only. No processing-time or wall-clock fields.
7. Do not use `multipleOf` for decimals (float math makes `57.3` fail `multipleOf: 0.1`). Do not rely on `allOf`
   to add properties under `additionalProperties: false`. Do not rely on `format` for validation; use `pattern`.
