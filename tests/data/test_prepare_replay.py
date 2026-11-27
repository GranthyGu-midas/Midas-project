"""Offline transformation invariants; no engine, network or credentials required."""
from contextlib import redirect_stdout
from decimal import Decimal
import gzip
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[2] / "src/scripts/prepare_replay.py"
spec = importlib.util.spec_from_file_location("prepare_replay", SCRIPT)
replay = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = replay
spec.loader.exec_module(replay)

START = "2026-11-23T13:55:00.000Z"
END = "2026-11-23T14:00:00.000Z"
T1 = "2026-11-23T13:55:00.007Z"
MAKER = "ac928c66-ca53-498f-9c13-a110027a60e8"
TAKER = "132fb6ae-456b-4654-b4e0-d681ac05cea1"


def snapshot_order(oid, side, price, size, time=START):
    return dict(oid=oid, side=side, price=price, size=size, timestamp=time)


def delta(seq, oid=MAKER, kind="update", size="8", side="A", price="101", time=T1):
    return dict(oid=oid, seq=seq, block_number=11, timestamp=time, diff_type=kind,
                new_size=size, side=side, price=price, coin="BTC")


def lifecycle(oid, status="filled", size="0", original="2", tif="Ioc", trigger=False, time=T1):
    return dict(oid=oid, seq=0, block_number=11, timestamp=time, coin="BTC", side="B",
                status=status, size=size, orig_size=original, limit_price="101", tif=tif,
                is_trigger=trigger, order_type="Limit")


def trade_pair(size="2"):
    common = dict(trade_id="trade-1", timestamp=T1, coin="BTC", price="101", size=size)
    return [{**common, "order_id": MAKER, "side": "A", "crossed": False},
            {**common, "order_id": TAKER, "side": "B", "crossed": True}]


class PreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "raw"

    def fixture(self, diffs=(), trades=(), lifecycle_rows=(), initial_size="10", extra_asks=()):
        markets = {}
        for symbol in ("BTC", "ETH"):
            path = self.source / symbol
            path.mkdir(parents=True, exist_ok=True)
            snapshot = dict(last_block_number=10,
                bids=[snapshot_order("bid", "B", "99", "10")],
                asks=[snapshot_order(MAKER, "A", "101", initial_size), *extra_asks])
            snapshot.update(bid_count=len(snapshot["bids"]), ask_count=len(snapshot["asks"]))
            (path / "snapshot_start.json").write_text(json.dumps(dict(success=True, data=snapshot)))
            datasets = {}
            for name, rows in (("l4_diffs", diffs), ("trades", trades), ("order_events", lifecycle_rows)):
                folder = path / name
                folder.mkdir(exist_ok=True)
                data = [{**r, "coin": symbol} for r in rows]
                with gzip.open(folder / "page-000000.json.gz", "wt") as stream:
                    json.dump(dict(success=True, data=data), stream)
                datasets[name] = dict(pagination_complete=True, rows=len(data))
            markets[symbol] = dict(datasets=datasets)
        request = dict(venue="hyperliquid", symbols=["BTC", "ETH"], start=START, end=END, market_type="perpetual")
        (self.source / "request.json").write_text(json.dumps(request))
        (self.source / "manifest.json").write_text(json.dumps(dict(status="downloaded", markets=markets)))

    def adapter(self):
        return replay.HyperliquidAdapter(self.source, "BTC", 1, 1, replay.ns(START), replay.ns(END)).prepare()

    def test_decimal_is_exact_and_invalid_precision_is_rejected(self):
        self.assertEqual(replay.fixed(Decimal("0.00000001")), 1)
        self.assertEqual(replay.fixed("61103.01"), 6110301000000)
        for value in ("0.000000001", "NaN", "Infinity", "-1", "92233720369"):
            with self.assertRaises(ValueError):
                replay.fixed(value)

    def test_timestamp_preserves_nine_digits(self):
        self.assertEqual(replay.ns("2026-11-23T13:55:00.987654321Z") - replay.ns(START), 987654321)
        self.assertEqual(replay.ns(START), replay.ns("2026-11-23T13:55:00+00:00"))

    def test_uuid_and_numeric_source_identity_share_same_id_contract(self):
        start = replay.ns(START)
        commands = [replay.Command("BTC", 1, 1, MAKER, "0", start, replay.NEW, replay.SELL,
                                  101 * replay.SCALE, replay.SCALE),
                    replay.Command("BTC", 1, 1, MAKER, "0", start, replay.CANCEL, replay.SELL),
                    replay.Command("ETH", 2, 2, "614016150072", "0", start, replay.NEW, replay.BUY,
                                  99 * replay.SCALE, replay.SCALE)]
        events, _ = replay.number_commands([], [commands[:2], commands[2:]], start, replay.ns(END))
        self.assertEqual(events[0].id, events[1].id)
        self.assertEqual(events[0].id, (1 << 48) | (1 << 47) | 1)
        self.assertEqual(events[2].id >> 48, 2)

    def test_nonmonotone_time_clamps_without_reordering_product(self):
        start = replay.ns(START)
        commands = [replay.Command("BTC", 1, 1, "o", "0", start + 100, replay.NEW, replay.BUY,
                                  99 * replay.SCALE, replay.SCALE),
                    replay.Command("BTC", 1, 1, "o", "0", start + 50, replay.CANCEL, replay.BUY)]
        result, _ = replay.number_commands([], [commands], start, replay.ns(END))
        self.assertEqual([c.offset for c in result], [100, 100])
        self.assertEqual(result[1].time, start + 50)
        self.assertTrue(result[1].flags & replay.TIME_CLAMPED)

    def test_new_before_cancel_and_distinct_replacement_id(self):
        start = replay.ns(START)
        bad = replay.Command("BTC", 1, 1, "o", "0", start, replay.CANCEL, replay.BUY)
        with self.assertRaisesRegex(ValueError, "Forward"):
            replay.number_commands([], [[bad]], start, replay.ns(END))
        commands = [replay.Command("BTC", 1, 1, "o", revision, start, replay.NEW, replay.BUY,
                                  99 * replay.SCALE, replay.SCALE) for revision in ("old", "new")]
        events, _ = replay.number_commands([], [commands], start, replay.ns(END))
        self.assertNotEqual(events[0].id, events[1].id)

    def test_fill_and_zero_terminal_remove_do_not_generate_cancel(self):
        self.fixture([delta(0, size="0"), delta(1, kind="remove", size=None)], trade_pair(),
                     [lifecycle(TAKER)], initial_size="2")
        adapter = self.adapter()
        self.assertEqual(len(adapter.commands), 1)
        c = adapter.commands[0]
        self.assertEqual((c.key, c.operation, c.tif, c.quantity), (TAKER, replay.NEW, replay.IOC, 2 * replay.SCALE))
        self.assertEqual(adapter.stats["fill_decrements_suppressed"], 1)

    def test_aggressive_part_and_rest_are_one_order(self):
        self.fixture([delta(0, size="0"), delta(1, kind="remove", size=None),
                      delta(2, TAKER, kind="new", size="3", side="B")], trade_pair(),
                     [lifecycle(TAKER, status="open", size="3", original="5", tif="Gtc")], initial_size="2")
        adapter = self.adapter()
        self.assertEqual(len(adapter.commands), 1)
        self.assertEqual((adapter.commands[0].quantity, adapter.commands[0].tif), (5 * replay.SCALE, replay.GTC))
        self.assertEqual(adapter.stats["resting_remainders_merged"], 1)

    def test_nontrade_reduction_before_same_time_trade_is_not_double_counted(self):
        self.fixture([delta(0, size="7"), delta(1, size="5")], trade_pair(), [lifecycle(TAKER)])
        adapter = self.adapter()
        self.assertEqual([c.operation for c in adapter.commands], [replay.REDUCE, replay.NEW])
        self.assertEqual(adapter.commands[0].quantity, 3 * replay.SCALE)
        self.assertEqual(adapter.commands[1].quantity, 2 * replay.SCALE)

    def test_inferred_aggressor_keeps_observed_volume_and_limit(self):
        self.fixture([delta(0)], trade_pair())
        c = self.adapter().commands[0]
        self.assertEqual((c.quantity, c.price), (2 * replay.SCALE, 101 * replay.SCALE))
        self.assertTrue(c.flags & replay.INFERRED_PARAMS)

    def test_partial_ioc_keeps_original_submission_quantity(self):
        self.fixture([delta(0)], trade_pair(), [lifecycle(TAKER, original="4")])
        self.assertEqual(self.adapter().commands[0].quantity, 4 * replay.SCALE)

    def test_snapshot_fifo_uses_timestamp_not_numeric_id(self):
        extra = snapshot_order("z-earlier", "A", "101", "1", "2026-11-23T13:54:59.000Z")
        self.fixture(extra_asks=[extra])
        asks = [c.key for c in self.adapter().bootstrap if c.side == replay.SELL]
        self.assertEqual(asks, ["z-earlier", MAKER])

    def test_dormant_trigger_is_audited_not_sent(self):
        self.fixture(lifecycle_rows=[lifecycle("trigger", status="open", trigger=True)])
        adapter = self.adapter()
        self.assertEqual(adapter.commands, [])
        self.assertEqual(adapter.exclusions[0]["source_order_id"], "trigger")

    def test_unpaired_or_unmatched_trades_fail_closed(self):
        for rows in (trade_pair()[:1], trade_pair("3")):
            with self.subTest(rows=rows):
                self.fixture([delta(0)], rows)
                with self.assertRaises(ValueError):
                    self.adapter()

    def test_conflicting_duplicate_source_rows_are_rejected(self):
        self.fixture([delta(0), delta(0, size="7")])
        with self.assertRaisesRegex(ValueError, "Conflicting duplicate"):
            self.adapter()

    def test_identical_overlapping_trade_rows_are_deduplicated(self):
        self.fixture([delta(0)], trade_pair() + trade_pair(), [lifecycle(TAKER)])
        adapter = self.adapter()
        self.assertEqual(adapter.stats["trades_duplicates"], 2)
        self.assertEqual(len(adapter.commands), 1)
        self.assertEqual(len(adapter.fills), 1)

    def test_equal_size_ambiguity_is_explicit(self):
        second = [{**r, "trade_id": "trade-2"} for r in trade_pair()]
        self.fixture([delta(0, size="8"), delta(1, size="6"), delta(2, size="4")],
                     trade_pair() + second, [lifecycle(TAKER, original="4")])
        adapter = self.adapter()
        self.assertTrue(adapter.fills[0].ambiguous)
        self.assertEqual(adapter.stats["unique_trades"], 2)
        self.assertEqual([c.operation for c in adapter.commands], [replay.NEW, replay.REDUCE])
        self.assertEqual(adapter.commands[0].quantity, 4 * replay.SCALE)
        self.assertEqual(adapter.commands[1].quantity, 2 * replay.SCALE)

    def test_increase_becomes_cancel_replace(self):
        self.fixture([delta(0, size="12")])
        commands = self.adapter().commands
        self.assertEqual([c.operation for c in commands], [replay.CANCEL, replay.NEW])
        self.assertNotEqual(commands[0].generation, commands[1].generation)

    def test_bundle_roundtrip_deterministic_and_tamper_detection(self):
        self.fixture([delta(0), delta(1, kind="remove", size=None)], trade_pair(), [lifecycle(TAKER)])
        source_hash = replay.sha256(self.source / "BTC/snapshot_start.json")
        first, second = self.root / "first", self.root / "second"
        with redirect_stdout(io.StringIO()):
            manifest = replay.write_bundle(self.source, first)
            replay.write_bundle(self.source, second)
        self.assertTrue(replay.validate_bundle(first)["passed"])
        self.assertEqual(manifest["counts"]["operations"], {"NEW": 2, "CANCEL": 2})
        for path in first.iterdir():
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes(), path.name)
        self.assertEqual(source_hash, replay.sha256(self.source / "BTC/snapshot_start.json"))
        with self.assertRaisesRegex(ValueError, "already exists"):
            replay.write_bundle(self.source, first)
        binary = first / "events.bin"
        binary.write_bytes(binary.read_bytes()[:-1])
        with self.assertRaisesRegex(ValueError, "Checksum"):
            replay.validate_bundle(first)
        # Even if the manifest is rehashed, the binary framing check catches it.
        stored = json.loads((first / "manifest.json").read_text())
        stored["files"]["events.bin"] = dict(bytes=binary.stat().st_size, sha256=replay.sha256(binary))
        replay.write_json(first / "manifest.json", stored)
        with self.assertRaisesRegex(ValueError, "Truncated"):
            replay.validate_bundle(first)

    def test_audit_catches_a_reference_trade_removed_from_reconstruction(self):
        self.fixture([delta(0)], trade_pair(), [lifecycle(TAKER)])
        destination = self.root / "bundle"
        with redirect_stdout(io.StringIO()):
            replay.write_bundle(self.source, destination)
        path = destination / "provenance.jsonl.gz"
        with gzip.open(path, "rt") as stream:
            rows = [json.loads(line) for line in stream]
        for row in rows:
            if row["source_kind"] == "aggressor_reconstruction":
                row["trade_ids"] = []
                row["observed_fill_quantity_e8"] = 0
        with replay.json_lines(path) as write:
            for row in rows:
                write(row)
        manifest = json.loads((destination / "manifest.json").read_text())
        manifest["files"][path.name] = dict(bytes=path.stat().st_size, sha256=replay.sha256(path))
        replay.write_json(destination / "manifest.json", manifest)
        with self.assertRaisesRegex(ValueError, "Historical executions omitted"):
            replay.validate_bundle(destination)


if __name__ == "__main__":
    unittest.main()
