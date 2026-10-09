"""The generator is deterministic, chunkable, messy on purpose, and keeps the contracts the pipeline relies on."""
import filecmp
import glob
import json
import os
from collections import defaultdict

from bronze_to_served.stagedoor.datagen import GeneratorSettings, landed_through, read_jsonl, write_days
from reference.oracle import order_failures, parse_order
from reference.worlds import landing

TINY = GeneratorSettings(customers=150, days=40, clickstream_scale=0.5)


def _files(root):
    root = str(root)
    return sorted(os.path.relpath(p, root) for p in glob.glob(os.path.join(root, "**", "*.*"), recursive=True)
                  if not p.endswith("_landing_state.json"))


def _same_tree(a, b):
    assert _files(a) == _files(b)
    for rel in _files(a):
        assert filecmp.cmp(os.path.join(a, rel), os.path.join(b, rel), shallow=False), rel


def test_same_seed_gives_byte_identical_files(tmp_path):
    write_days(TINY, str(tmp_path / "a"), 0, 39)
    write_days(TINY, str(tmp_path / "b"), 0, 39)
    assert len(_files(tmp_path / "a")) > 100
    _same_tree(str(tmp_path / "a"), str(tmp_path / "b"))


def test_landing_in_chunks_equals_landing_at_once(tmp_path):
    whole, chunks = str(tmp_path / "whole"), str(tmp_path / "chunks")
    write_days(TINY, whole, 0, 39)
    write_days(TINY, chunks, 0, 19)
    write_days(TINY, chunks, 20, 39)
    _same_tree(whole, chunks)
    assert landed_through(chunks) == 39


def _source(name):
    return sorted(glob.glob(os.path.join(landing(), name, "*.json")))


def test_order_lifecycle_moves_forward_and_lines_never_change():
    by_order = defaultdict(list)
    for raw in read_jsonl(_source("orders")):
        o = parse_order(raw)
        if not order_failures(o):
            by_order[o["order_id"]].append(o)
    rank = {"PLACED": 0, "PAID": 1, "CANCELLED": 1, "REFUNDED": 2}
    assert len(by_order) > 1000
    for order_id, events in by_order.items():
        timeline = sorted({(e["updated_at"], e["status"]) for e in events})
        assert [rank[s] for _, s in timeline] == sorted(rank[s] for _, s in timeline), order_id
        lines = {json.dumps([(i["line_no"], i["quantity"], str(i["unit_price"])) for i in e["items"]]) for e in events}
        assert len(lines) == 1, order_id


def test_duplicates_are_exact_copies():
    groups = defaultdict(list)
    for raw in read_jsonl(_source("orders")):
        groups[(raw["order_id"], raw["status"], raw["updated_at"])].append(json.dumps(raw, sort_keys=True))
    duplicated = [v for v in groups.values() if len(v) > 1]
    assert duplicated, "expected some re-delivered order events"
    assert all(len(set(v)) == 1 for v in duplicated)


def test_some_records_are_invalid_or_late():
    invalid = sum(1 for raw in read_jsonl(_source("orders")) if order_failures(parse_order(raw)))
    late = 0
    for path in _source("clickstream"):
        stamp = os.path.basename(path)[len("clickstream_"):-len(".json")]
        late += sum(1 for r in read_jsonl([path]) if r.get("event_ts") and r["event_ts"][:10].replace("-", "") < stamp)
    assert invalid > 0 and late > 0


def test_cdc_events_never_go_backwards_across_files():
    seen, last, contents = set(), {}, {}
    for path in _source("customers_cdc"):
        rows = [r for r in read_jsonl([path]) if isinstance(r.get("lsn"), int) and r.get("customer_id")]
        for r in rows:
            key = (r["customer_id"], r["lsn"])
            if r["lsn"] <= last.get(r["customer_id"], -1):
                assert key in seen, f"{key} arrived after a later change without being a re-delivery"
            assert contents.setdefault(key, json.dumps(r, sort_keys=True)) == json.dumps(r, sort_keys=True)
        for r in rows:
            seen.add((r["customer_id"], r["lsn"]))
            last[r["customer_id"]] = max(last.get(r["customer_id"], -1), r["lsn"])


def test_schema_drift_appears_halfway():
    files = _source("orders")
    assert not any("seat_section" in r for r in read_jsonl(files[:5]))
    assert any("seat_section" in r for r in read_jsonl(files[-5:]))


def test_reference_snapshots_are_weekly_and_quarterly():
    events = glob.glob(os.path.join(landing(), "reference", "events_*.csv"))
    venues = glob.glob(os.path.join(landing(), "reference", "venues_*.csv"))
    assert 40 <= len(events) <= 46 and 3 <= len(venues) <= 5
