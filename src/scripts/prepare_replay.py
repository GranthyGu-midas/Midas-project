#!/usr/bin/env python3
"""Build an offline, versioned Q input bundle; never connect to an exchange/engine.

Usage: python3 src/scripts/prepare_replay.py SOURCE --output DESTINATION
       python3 src/scripts/prepare_replay.py --validate DESTINATION

Only the source adapter understands Hyperliquid. Command, write_bundle and the
binary contract use venue-independent IDs, timestamps and order operations.
No third-party packages are required. Existing output directories are refused.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict, deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import shutil
import struct
import tempfile

SCALE = 100_000_000
Q_BIT = 1 << 47
ORDINAL_LIMIT = Q_BIT - 1
NEW, CANCEL, REDUCE = 1, 2, 3
BUY, SELL = 1, 2
GTC, IOC = 0, 1
BOOTSTRAP, INFERRED_ORDER, INFERRED_PARAMS, TRANSFORMED, TIME_CLAMPED = 1, 2, 4, 8, 16
MAGIC = b"MIDASQ01"
HEADER = struct.Struct("<8sHHIQQ32s")
RECORD = struct.Struct("<QQQQqqHHHBBBB6x")
assert HEADER.size == RECORD.size == 64
FIELDS = ("input_seq", "event_time_ns", "offset_ns", "order_id", "price_e8", "quantity_e8",
          "shard_id", "symbol_id", "actor_id", "operation", "side", "tif", "flags")
SOURCE_VERSION = "hyperliquid-book-and-fills-v1"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def ns(text):
    """Parse UTC ISO timestamps without float or microsecond truncation."""
    match = re.fullmatch(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(?:Z|\+00:00)", text)
    require(match is not None, f"Not a supported UTC timestamp: {text!r}")
    dt = datetime.strptime(match[1], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc) - EPOCH
    return (dt.days * 86400 + dt.seconds) * 1_000_000_000 + int((match[2] or "").ljust(9, "0"))


def fixed(value):
    scaled = Decimal(str(value)) * SCALE
    require(scaled.is_finite() and scaled == scaled.to_integral_value(),
            f"Value cannot be represented exactly at 1e-8: {value}")
    result = int(scaled)
    require(0 <= result < 1 << 63, f"Value outside nonnegative int64: {value}")
    return result


def order_id(shard, ordinal):
    require(0 < shard < 1 << 16 and 0 < ordinal <= ORDINAL_LIMIT, "Order ID range exhausted")
    return (shard << 48) | Q_BIT | ordinal


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_text(value):
    # Decimal values in audit metadata are strings, never binary floating point.
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, indent=2) + "\n",
                    encoding="utf-8")


@contextmanager
def json_lines(path):
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as zipped:
            with io.TextIOWrapper(zipped, encoding="utf-8", newline="\n") as stream:
                yield lambda row: stream.write(json_text(row) + "\n")


@dataclass(slots=True)
class Command:
    symbol: str
    shard: int
    symbol_id: int
    key: str  # Source identity is opaque; UUIDs and numeric strings both work.
    generation: str
    time: int
    operation: int
    side: int
    price: int = 0
    quantity: int = 0
    tif: int = GTC
    flags: int = 0
    evidence: dict = field(default_factory=dict)
    offset: int = 0
    sequence: int = 0
    id: int = 0

    @property
    def identity(self):
        return self.symbol, self.key, self.generation

    @property
    def actor(self):
        return (self.symbol_id - 1) * 2 + self.side

    def values(self):
        return (self.sequence, self.time, self.offset, self.id, self.price, self.quantity,
                self.shard, self.symbol_id, self.actor, self.operation, self.side, self.tif, self.flags)


@dataclass(slots=True)
class Delta:
    key: str
    time: int
    block: int
    seq: int
    side: int
    price: int
    size: int
    kind: str
    ref: str
    old_size: int = 0
    old_price: int = 0
    fill: object = None


@dataclass(slots=True)
class Fill:
    id: str
    time: int
    maker: str
    taker: str
    side: int  # aggressor side
    price: int
    size: int
    refs: tuple
    position: int = -1
    ambiguous: bool = False


class HyperliquidAdapter:
    """Associate observed book changes with paired fills, then reconstruct input.

    Diff seq and lifecycle seq are distinct streams and are never compared.
    Fills have no common sequence: association uses time, maker, price and exact
    size. Ambiguous equal-size associations are stable and explicitly reported.
    """
    def __init__(self, source, symbol, shard, symbol_id, start, end):
        self.source, self.symbol = Path(source), symbol
        self.shard, self.symbol_id, self.start, self.end = shard, symbol_id, start, end
        self.files = {}
        self.stats = Counter()
        self.status_counts = Counter()
        self.exclusions = []
        self.lifecycles = defaultdict(list)
        self.bootstrap = []
        self.commands = []
        self.fills = []

    def load(self, relative):
        path = self.source / relative
        self.files[str(relative)] = {"sha256": sha256(path), "bytes": path.stat().st_size}
        with (gzip.open(path, "rt", encoding="utf-8") if path.suffix == ".gz" else path.open(encoding="utf-8")) as stream:
            envelope = json.load(stream, parse_float=Decimal)
        require(envelope.get("success") is True, f"Unsuccessful source response: {relative}")
        return envelope["data"]

    def rows(self, kind):
        pages = sorted((self.source / self.symbol / kind).glob("page-*.json.gz"))
        require(pages, f"No pages for {self.symbol}/{kind}")
        seen = {}
        for index, path in enumerate(pages):
            require(path.name == f"page-{index:06d}.json.gz", f"Noncontiguous pages: {path}")
            relative = path.relative_to(self.source)
            for row_index, row in enumerate(self.load(relative)):
                key = ((str(row["trade_id"]), bool(row["crossed"])) if kind == "trades"
                       else (row["block_number"], row["seq"]))
                digest = hashlib.sha256(json_text(row).encode()).digest()
                if key in seen:
                    require(seen[key] == digest, f"Conflicting duplicate in {kind}: {key}")
                    self.stats[f"{kind}_duplicates"] += 1
                    continue
                seen[key] = digest
                self.stats[f"{kind}_unique_rows"] += 1
                yield row, f"{relative.as_posix()}#data/{row_index}"

    def command(self, key, generation, time, op, side, price=0, quantity=0, tif=GTC, flags=0, **evidence):
        return Command(self.symbol, self.shard, self.symbol_id, str(key), generation, time,
                       op, side, price, quantity, tif, flags, evidence)

    def prepare(self):
        snapshot = self.load(Path(self.symbol) / "snapshot_start.json")
        self.snapshot_block = snapshot["last_block_number"]
        initial = []
        for label, side in (("bids", BUY), ("asks", SELL)):
            require(len(snapshot[label]) == snapshot[label[:-1] + "_count"], "Snapshot count mismatch")
            for rank, row in enumerate(snapshot[label]):
                price, size = fixed(row["price"]), fixed(row["size"])
                require(price > 0 and size > 0, "Nonpositive initial order")
                initial.append((side, price, ns(row["timestamp"]), rank, str(row["oid"]), size))
        # Standard FIFO seed: observed timestamp, then source position within price.
        initial.sort(key=lambda x: (x[0], -x[1] if x[0] == BUY else x[1], x[2], x[3]))
        state = {}
        for side, price, timestamp, rank, key, size in initial:
            require(key not in state, f"Duplicate initial ID {key}")
            state[key] = (size, price, side)
            self.bootstrap.append(self.command(key, "initial", self.start, NEW, side, price, size,
                flags=BOOTSTRAP, source_kind="snapshot", snapshot_block=self.snapshot_block,
                source_time_ns=str(timestamp), source_rank=rank,
                source_ref=f"{self.symbol}/snapshot_start.json#data/{'bids' if side == BUY else 'asks'}/{rank}"))
        require(max(x[1] for x in initial if x[0] == BUY) < min(x[1] for x in initial if x[0] == SELL),
                "Initial book is crossed")
        self.stats["initial_orders"] = len(initial)
        for row, ref in self.rows("order_events"):
            if row["block_number"] <= self.snapshot_block:
                self.stats["lifecycle_before_snapshot"] += 1
                continue
            timestamp = ns(row["timestamp"])
            require(self.start <= timestamp < self.end, f"Lifecycle outside window: {ref}")
            require(row["coin"] == self.symbol, f"Wrong symbol: {ref}")
            self.status_counts[row["status"]] += 1
            self.lifecycles[str(row["oid"])].append({
                "time": timestamp, "block": row["block_number"], "seq": row["seq"],
                "status": row["status"], "tif": row.get("tif"), "side": BUY if row["side"] == "B" else SELL,
                "price": fixed(row["limit_price"]), "size": fixed(row["size"]),
                "original_size": fixed(row["orig_size"]), "trigger": bool(row.get("is_trigger")),
                "order_type": row.get("order_type"), "ref": ref})
        for events in self.lifecycles.values():
            events.sort(key=lambda x: (x["block"], x["seq"]))

        deltas, drops, new_at = [], defaultdict(deque), {}
        previous = (-1, -1)
        births = {key: -1 for key in state}
        deaths = {}
        for row, ref in self.rows("l4_diffs"):
            position = row["block_number"], row["seq"]
            require(position > previous, f"Unordered diff sequence: {ref}")
            previous = position
            if row["block_number"] <= self.snapshot_block:
                self.stats["diffs_before_snapshot"] += 1
                continue
            timestamp = ns(row["timestamp"])
            require(self.start <= timestamp < self.end, f"Diff outside window: {ref}")
            require(row["coin"] == self.symbol and row["side"] in ("A", "B"), f"Wrong product/side: {ref}")
            kind = row["diff_type"]
            require(kind in ("new", "update", "remove"), f"Unknown diff kind: {kind}")
            d = Delta(str(row["oid"]), timestamp, row["block_number"], row["seq"],
                      BUY if row["side"] == "B" else SELL, fixed(row["price"]),
                      0 if kind == "remove" else fixed(row["new_size"]), kind, ref)
            index = len(deltas)
            if kind == "new":
                require(d.key not in state and d.size > 0, f"Invalid insertion: {ref}")
                require(d.key not in births, f"Source reused order ID; needs explicit revision adapter: {d.key}")
                births[d.key] = index
                new_at[d.time, d.key] = index
            else:
                require(d.key in state, f"Unknown source order: {ref}")
                d.old_size, d.old_price, old_side = state[d.key]
                require(old_side == d.side, f"Side changed in place: {ref}")
                if d.old_size > d.size and d.old_price == d.price:
                    drops[d.time, d.key, d.price, d.old_size - d.size].append(index)
            if kind == "remove":
                del state[d.key]
                deaths[d.key] = index
            else:
                state[d.key] = (d.size, d.price, d.side)  # keep zero until the remove message
            deltas.append(d)
            self.stats[f"diff_{kind}"] += 1
            self.stats["ignored_insert_before"] += int(row.get("insert_before") is not None)

        pairs = defaultdict(dict)
        for row, ref in self.rows("trades"):
            require(isinstance(row["crossed"], bool), f"Trade crossed must be boolean: {ref}")
            timestamp = ns(row["timestamp"])
            if timestamp < self.start or timestamp >= self.end:
                self.stats["trades_outside_window"] += 1
                continue
            require(row["coin"] == self.symbol and row["side"] in ("A", "B"), f"Wrong trade product/side: {ref}")
            pairs[str(row["trade_id"])][bool(row["crossed"])] = (row, ref)
        takes = defaultdict(list)
        for trade_id, pair in pairs.items():
            require(set(pair) == {False, True}, f"Incomplete maker/taker pair: {trade_id}")
            maker, maker_ref = pair[False]
            taker, taker_ref = pair[True]
            require(maker["timestamp"] == taker["timestamp"] and maker["side"] != taker["side"] and
                    fixed(maker["size"]) == fixed(taker["size"]) and fixed(maker["price"]) == fixed(taker["price"]),
                    f"Inconsistent trade pair: {trade_id}")
            fill = Fill(trade_id, ns(maker["timestamp"]), str(maker["order_id"]), str(taker["order_id"]),
                        BUY if taker["side"] == "B" else SELL, fixed(maker["price"]), fixed(maker["size"]),
                        (maker_ref, taker_ref))
            candidates = drops[fill.time, fill.maker, fill.price, fill.size]
            require(candidates and fill.size > 0, f"Trade has no exact maker decrement: {trade_id}")
            fill.ambiguous = len(candidates) > 1
            fill.position = candidates.popleft()
            require(deltas[fill.position].side != fill.side, "Maker side disagrees with trade")
            deltas[fill.position].fill = fill
            self.fills.append(fill)
            takes[fill.time, fill.taker].append(fill)
            self.stats["ambiguous_fill_associations"] += int(fill.ambiguous)
        self.stats["unique_trades"] = len(self.fills)
        self.stats["fill_decrements_suppressed"] = len(self.fills)

        arrivals = defaultdict(list)
        resting_takers = {}
        for (timestamp, key), fills in takes.items():
            fills.sort(key=lambda x: x.position)
            anchor = fills[0].position
            require(all(births[f.maker] < anchor <= deaths.get(f.maker, len(deltas)) for f in fills),
                    f"Aggressor spans a later maker insertion; cannot infer atomic order: {key}")
            require(key not in births or births[key] >= anchor,
                    f"Existing resting order becomes aggressor: {key}")
            same_time = [e for e in self.lifecycles.get(key, []) if e["time"] == timestamp]
            metadata = next((e for e in same_time if not e["trigger"] and e["original_size"]), None)
            side = fills[0].side
            require(all(f.side == side for f in fills), f"Aggressor side changed: {key}")
            quantity = sum(f.size for f in fills)
            worst_price = (max if side == BUY else min)(f.price for f in fills)
            price, tif, flags = worst_price, IOC, INFERRED_ORDER
            new_index = new_at.get((timestamp, key))
            generation = f"aggressor:{timestamp}"
            if new_index is not None:
                require(new_index > fills[-1].position, f"Resting update precedes taker fills: {key}")
                residual = deltas[new_index]
                price, tif = residual.price, GTC
                quantity += residual.size
                resting_takers[new_index] = generation
                self.stats["aggressors_with_resting_remainder"] += 1
            elif metadata is not None:
                if metadata["original_size"] >= quantity:
                    quantity = metadata["original_size"]
                else:
                    flags |= INFERRED_PARAMS
                price = metadata["price"]
                tif = GTC if metadata["tif"] in ("Gtc", "Alo") else IOC
            else:
                flags |= INFERRED_PARAMS
                self.stats["aggressors_without_submission_metadata"] += 1
            if price <= 0 or (side == BUY and price < worst_price) or (side == SELL and price > worst_price):
                price = worst_price
                flags |= INFERRED_PARAMS
            if metadata and (metadata["tif"] not in ("Gtc", "Ioc") or metadata["order_type"] != "Limit"):
                flags |= TRANSFORMED
            arrivals[anchor].append(self.command(key, generation, timestamp, NEW, side, price, quantity, tif, flags,
                source_kind="aggressor_reconstruction", source_ref=deltas[anchor].ref,
                trade_ids=[f.id for f in fills], metadata=metadata,
                observed_fill_quantity_e8=sum(f.size for f in fills),
                observed_resting_quantity_e8=deltas[new_index].size if new_index is not None else 0))

        bindings = {c.key: c.generation for c in self.bootstrap}
        represented = set(bindings)
        for index, d in enumerate(deltas):
            for incoming in arrivals[index]:
                require(incoming.key not in bindings, f"Duplicate aggressor identity: {incoming.key}")
                bindings[incoming.key] = incoming.generation
                represented.add(incoming.key)
                self.commands.append(incoming)
            evidence = dict(source_kind="book_change", source_ref=d.ref, source_block=d.block,
                            source_seq=d.seq, source_diff=d.kind)
            if d.kind == "new":
                if index in resting_takers:
                    require(bindings[d.key] == resting_takers[index], "Missing taker creation")
                    self.stats["resting_remainders_merged"] += 1
                else:
                    generation = f"rest:{index}"
                    bindings[d.key] = generation
                    self.commands.append(self.command(d.key, generation, d.time, NEW, d.side, d.price, d.size,
                        flags=INFERRED_ORDER | TRANSFORMED, **evidence))
                    represented.add(d.key)
            elif d.fill is not None:
                pass  # Execution outcomes are never applied a second time.
            elif d.kind == "update" and (d.size > d.old_size or d.price != d.old_price):
                self.commands.append(self.command(d.key, bindings[d.key], d.time, CANCEL, d.side,
                                                 flags=TRANSFORMED, **evidence))
                bindings[d.key] = f"replace:{index}"
                self.commands.append(self.command(d.key, bindings[d.key], d.time, NEW, d.side, d.price, d.size,
                                                 flags=TRANSFORMED, **evidence))
                self.stats["cancel_replace_pairs"] += 1
            elif d.old_size > d.size:
                op = CANCEL if d.kind == "remove" else REDUCE
                statuses = [e["status"] for e in self.lifecycles.get(d.key, []) if e["time"] == d.time]
                self.commands.append(self.command(d.key, bindings[d.key], d.time, op, d.side,
                    quantity=d.old_size - d.size if op == REDUCE else 0,
                    flags=INFERRED_ORDER | TRANSFORMED, source_statuses=statuses,
                    conversion="nontrade_book_removal" if op == CANCEL else "nontrade_size_reduction", **evidence))
                self.stats["nontrade_cancels" if op == CANCEL else "nontrade_reductions"] += 1
            else:
                self.stats["zero_size_or_noop_outcomes_suppressed"] += 1

        # Dormant trigger state is not an executable book order. Every other
        # unexplained lifecycle is an error, rather than silently dropping it.
        for key, events in self.lifecycles.items():
            if key not in represented:
                require(all(e["trigger"] for e in events), f"Unrepresented non-trigger lifecycle: {key}")
                self.exclusions.append({"symbol": self.symbol, "source_order_id": key,
                    "reason": "trigger_state_without_visible_book_or_trade", "events": events})
        self.stats["excluded_trigger_order_ids"] = len(self.exclusions)
        self.stats["excluded_trigger_lifecycle_rows"] = sum(len(x["events"]) for x in self.exclusions)
        self.stats["source_active_orders_after_diffs"] = len(state)
        self.stats["commands"] = len(self.commands)
        self.stats["snapshot_block"] = self.snapshot_block
        self.stats["final_diff_block"] = deltas[-1].block if deltas else self.snapshot_block
        return self


def number_commands(bootstrap, per_symbol, start, end):
    """Common encoder: no venue-specific IDs, fields or sequencing rules."""
    events = []
    for commands in per_symbol:
        previous = 0
        for local_seq, c in enumerate(commands):
            require(start <= c.time < end, "Command time outside replay window")
            c.offset = max(previous, c.time - start)
            if c.offset != c.time - start:
                c.flags |= TIME_CLAMPED
            previous = c.offset
            events.append((c.offset, c.symbol_id, local_seq, c))
    events.sort(key=lambda x: x[:3])
    ordered = [x[3] for x in events]
    ids, counters = {}, Counter()
    for sequence, c in enumerate([*bootstrap, *ordered], 1):
        require(c.side in (BUY, SELL), "Invalid side")
        if c.operation == NEW:
            require(c.identity not in ids and c.price > 0 and c.quantity > 0, "Invalid/duplicate new order")
            require(c.price % 1_000_000 == 0, "Price does not fit configured 0.01 tick")
            require(c.quantity < 1 << 63 and c.price < 1 << 63, "Fixed-point overflow")
            counters[c.shard] += 1
            ids[c.identity] = order_id(c.shard, counters[c.shard])
        else:
            require(c.identity in ids, f"Forward or unknown order reference: {c.identity}")
        c.id, c.sequence = ids[c.identity], sequence
    return ordered, counters


FORMAT = """Midas normalized order flow, version 1

This bundle is OFFLINE INPUT, not the current engine wire protocol or a WAL.
The future Q adapter must bind logical actors, send preassigned IDs, translate
REDUCE_BY, and wait for durable bootstrap/end barriers. Engine support is not
implemented by this data preparation tool.

Files
  bootstrap.bin: submit every record, then wait for BOTH shards to commit.
  events.bin: one merged BTC/ETH stream, in file order, after bootstrap barrier.
  commands.jsonl.gz: readable mirror of both binary files (phase field).
  orders.jsonl.gz: normalized ID -> source ID/generation/initial command.
  provenance.jsonl.gz: per-command source evidence and reconstruction details.
  reference_trades.jsonl.gz: one row per historical trade, NOT input commands.
  excluded_lifecycles.jsonl.gz: non-executable trigger state kept for audit.
  manifest.json, validation.json: contract, checksums, counts and validation.

All binary integers are little-endian. No native C++ struct padding is implied.
Header (64 bytes): <8sHHIQQ32s
  offset 0 magic[8]='MIDASQ01'; 8 version:u16=1; 10 header_bytes:u16=64;
  12 record_bytes:u32=64; 16 count:u64; 24 window_start_unix_ns:u64;
  32 dataset_sha256[32]. Exactly count records follow; trailing bytes forbidden.
Record (64 bytes): <QQQQqqHHHBBBB6x
  0 input_seq:u64; 8 event_time_unix_ns:u64; 16 schedule_offset_ns:u64;
  24 order_id:u64; 32 price_e8:i64; 40 quantity_e8:i64;
  48 shard:u16; 50 symbol:u16; 52 logical_actor:u16;
  54 operation:u8; 55 side:u8; 56 tif:u8; 57 flags:u8; 58 reserved[6]=0.
Operations: 1 NEW, 2 CANCEL, 3 REDUCE_BY. Side: 1 BUY, 2 SELL.
TIF: 0 GTC, 1 IOC. CANCEL/REDUCE_BY have price=0, tif=0.
CANCEL quantity=0; REDUCE_BY quantity is the positive cancellation DELTA.
For REDUCE_BY the adapter reads the matching thread's current active order:
  removed=min(delta, remaining); newTotal=filled+remaining-removed.
Closed orders return failure. Never restore a historical remaining balance.
Flags: 1 bootstrap; 2 reconstructed ordering; 4 inferred price/quantity;
       8 source behavior normalized; 16 scheduling time clamped.
Prices/quantities are exact signed integers divided by 100000000.
Order ID: [shard:16][Q=1:1][ordinal:47], monotonically assigned on each shard.
Raw order IDs are opaque strings in sidecars; they may be UUIDs.
input_seq is contiguous across bootstrap THEN events, starting at 1.
It is an input identity, NOT engine FIFO priority or a durable acknowledgement.
Logical actor=(symbol_id-1)*2+side; bind to four distinct configured accounts.
The binary ordinal/ID is NOT the queue ordering authority during recovery.

Scheduling
  After bootstrap commits, set one shared monotonic-clock anchor for BTC/ETH.
  At speed=1, deadline=anchor+schedule_offset_ns. Do not wait for each reply.
  Equal deadlines retain file order; per-product order is preserved first.
  Original observed time is retained separately from scheduling offset.
  Source timestamps have millisecond precision; encoding ns invents no precision.
  Finish at manifest.duration_ns and wait for all submitted events to commit.
  Record lateness/backpressure. A dataset ID/input_seq does not itself implement
  recovery, exactly-once submission, deduplication or durable progress tracking.

Reconstruction policy
  Seed the visible initial book using remaining size, not original lifetime size.
  Initial same-price FIFO: observed order timestamp, then source snapshot rank;
  this is a deterministic simulation seed, not a claim of exchange queue parity.
  Pair maker/taker trade rows and associate EXACT maker decrements by observed
  time, maker ID, price and quantity; stable source order breaks equal-size ties.
  Generate ONE aggressor per (observed time, source taker ID) before its first
  maker decrement. Merge any same-time resting remainder into that one order.
  Use reported limit/quantity when present; otherwise infer executed size and
  worst execution price. These inputs are marked inferred and retain evidence.
  Other visible new orders become GTC. Special priority/post-only behavior is
  normalized. insert_before and wallet identities never affect replay routing.
  Trade decrements and zero-size terminal removes produce NO extra input.
  Remaining nontrade removals become CANCEL; nontrade decreases REDUCE_BY.
  Source-specific risk cancellations are represented as exogenous cancellations,
  not reimplemented exchange risk rules. Their source statuses are retained.
  Price/size increases become CANCEL+NEW with a new normalized generation.
  Dormant/ineffective trigger state is excluded with audit records.
  Cross-feed order timing is reconstructed, not an original submission trace.
  Historical fills are reference evidence, never forced matching outcomes.
  No periodic historical-book reconciliation or strategy behavior is included.
"""


def write_binary(path, commands, start, digest):
    with path.open("wb") as stream:
        stream.write(HEADER.pack(MAGIC, 1, HEADER.size, RECORD.size, len(commands), start, bytes.fromhex(digest)))
        for command in commands:
            stream.write(RECORD.pack(*command.values()))


def write_bundle(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    require(not destination.exists(), f"Output already exists: {destination}")
    require(source != destination and source not in destination.parents,
            "Output must be outside the immutable raw dataset directory")
    request_path, manifest_path = source / "request.json", source / "manifest.json"
    request = json.loads(request_path.read_text())
    downloaded = json.loads(manifest_path.read_text())
    require(downloaded.get("status") == "downloaded", "Raw download is not complete")
    require(downloaded.get("request", request) == request, "Download request/manifest disagree")
    require(request["venue"] == "hyperliquid", "This adapter accepts Hyperliquid raw envelopes")
    require(set(request["symbols"]) == {"BTC", "ETH"}, "This bundle expects BTC and ETH")
    start, end = ns(request["start"]), ns(request["end"])
    require(end > start, "Invalid window")
    adapters = []
    for symbol_id, symbol in enumerate(("BTC", "ETH"), 1):
        for kind in ("l4_diffs", "order_events", "trades"):
            require(downloaded["markets"][symbol]["datasets"][kind]["pagination_complete"], f"Incomplete {symbol}/{kind}")
        print(f"Preparing {symbol}: associating book, lifecycle and trades...", flush=True)
        adapter = HyperliquidAdapter(source, symbol, symbol_id, symbol_id, start, end).prepare()
        for kind in ("l4_diffs", "order_events", "trades"):
            reported = downloaded["markets"][symbol]["datasets"][kind]["rows"]
            require(adapter.stats[f"{kind}_unique_rows"] + adapter.stats[f"{kind}_duplicates"] == reported,
                    f"Downloaded manifest count disagrees: {symbol}/{kind}")
        adapters.append(adapter)
    bootstrap = [c for adapter in adapters for c in adapter.bootstrap]
    events, high_water = number_commands(bootstrap, [a.commands for a in adapters], start, end)
    inputs = {str(path.relative_to(source)): {"sha256": sha256(path), "bytes": path.stat().st_size}
              for path in (request_path, manifest_path)}
    for adapter in adapters:
        inputs.update(adapter.files)
    converter_hash = sha256(Path(__file__))
    identity = {"format": 1, "adapter": SOURCE_VERSION, "converter_sha256": converter_hash, "inputs": inputs,
                "start_ns": str(start), "end_ns": str(end), "scale": SCALE,
                "initial_ordering": "side-price-observed_time-source_rank"}
    digest = hashlib.sha256(json_text(identity).encode()).hexdigest()
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{destination.name}-", dir=destination.parent))
    try:
        write_binary(stage / "bootstrap.bin", bootstrap, start, digest)
        write_binary(stage / "events.bin", events, start, digest)
        with json_lines(stage / "commands.jsonl.gz") as readable, \
             json_lines(stage / "orders.jsonl.gz") as orders, \
             json_lines(stage / "provenance.jsonl.gz") as provenance:
            for phase, commands in (("bootstrap", bootstrap), ("replay", events)):
                for c in commands:
                    record = dict(zip(FIELDS, c.values()))
                    for name in ("event_time_ns", "offset_ns", "order_id"):
                        record[name] = str(record[name])
                    readable({"phase": phase, **record})
                    provenance({"input_seq": c.sequence, "order_id": str(c.id), "symbol": c.symbol,
                                "source_order_id": c.key, "generation": c.generation, **c.evidence})
                    if c.operation == NEW:
                        orders({"order_id": str(c.id), "source_order_id": c.key, "symbol": c.symbol,
                                "generation": c.generation, "first_input_seq": c.sequence,
                                "actor_id": c.actor, "side": c.side})
        with json_lines(stage / "reference_trades.jsonl.gz") as write:
            for adapter in adapters:
                for fill in sorted(adapter.fills, key=lambda x: x.position):
                    write({"symbol": adapter.symbol, "trade_id": fill.id, "event_time_ns": str(fill.time),
                           "maker_source_order_id": fill.maker, "taker_source_order_id": fill.taker,
                           "taker_side": fill.side, "price_e8": fill.price, "quantity_e8": fill.size,
                           "source_refs": fill.refs, "ambiguous_association": fill.ambiguous,
                           "usage": "reference_only_not_an_engine_command"})
        with json_lines(stage / "excluded_lifecycles.jsonl.gz") as write:
            for adapter in adapters:
                for row in adapter.exclusions:
                    write(row)
        (stage / "FORMAT.txt").write_text(FORMAT, encoding="utf-8")
        counts = {"bootstrap": len(bootstrap), "replay": len(events),
                  "operations": dict(Counter({1: "NEW", 2: "CANCEL", 3: "REDUCE_BY"}[c.operation] for c in events)),
                  "replay_tif": dict(Counter({0: "GTC", 1: "IOC"}[c.tif] for c in events if c.operation == NEW)),
                  "q_id_high_water": dict(high_water),
                  "time_clamped_events": sum(bool(c.flags & TIME_CLAMPED) for c in events)}
        manifest = {"format": "midas-normalized-order-flow", "version": 1, "status": "prepared",
            "dataset_id": digest, "adapter": SOURCE_VERSION, "converter_sha256": converter_hash, "source_path": str(source),
            "source_venue": request["venue"], "source_market_type": request["market_type"],
            "source_coverage": {s: downloaded["markets"][s].get("coverage") for s in ("BTC", "ETH")},
            "start": request["start"], "end_exclusive": request["end"],
            "start_ns": str(start), "end_ns": str(end), "duration_ns": str(end - start),
            "source_time_resolution_ns": 1_000_000, "numeric_scale": SCALE,
            "binary": {"header_bytes": HEADER.size, "record_bytes": RECORD.size, "endianness": "little",
                       "header_struct": HEADER.format, "record_struct": RECORD.format, "fields": FIELDS},
            "products": [{"symbol": a.symbol, "symbol_id": a.symbol_id, "shard_id": a.shard,
                          "price_tick_e8": 1_000_000, "quantity_step_e8": 1} for a in adapters],
            "actors": [{"actor_id": (i - 1) * 2 + side, "name": f"q_{s.lower()}_{label}",
                        "symbol": s, "side": side, "binding": "logical_only_configure_engine_user_later"}
                       for i, s in enumerate(("BTC", "ETH"), 1) for side, label in ((BUY, "buy"), (SELL, "sell"))],
            "counts": counts, "source_inputs": inputs,
            "per_symbol": {a.symbol: {"counts": dict(a.stats), "source_statuses": dict(a.status_counts)} for a in adapters},
            "contract": {"bootstrap": "Submit in file order and await both shards' durable barrier before starting clock",
                         "timing": "One monotonic anchor + offset_ns at speed=1; preserve file order; async replies",
                         "completion": "At duration_ns, await durable completion for all input_seq values",
                         "fifo": "Actual engine arrival sequence; order_id and input_seq are not recovery priority",
                         "matching": "Price priority and same-price FIFO; outcomes may differ from source",
                         "reduce": "Cancel min(quantity_e8,current_remaining) using current matching-thread state",
                         "reference_trades": "Never send as additional commands or forced fills"},
            "limitations": ["Cross-feed command order is inferred from observed effects; equal-size fill ties use stable source order",
                            "Source millisecond timestamps do not expose actual submission nanoseconds",
                            "Initial FIFO is a deterministic timestamp seed, not exchange queue reconstruction",
                            "Venue-specific order priority/risk/trigger rules are normalized, not implemented",
                            "Provider coverage is unverified; local associations do not prove the feed has no omissions",
                            "Q execution, matching correctness, runtime timing and durability are not tested by this offline tool"],
            "files": {}}
        for path in sorted(stage.iterdir()):
            manifest["files"][path.name] = {"bytes": path.stat().st_size, "sha256": sha256(path)}
        write_json(stage / "manifest.json", manifest)
        validation = validate_bundle(stage)
        write_json(stage / "validation.json", validation)
        manifest["files"]["validation.json"] = {"bytes": (stage / "validation.json").stat().st_size,
                                                "sha256": sha256(stage / "validation.json")}
        manifest["validation"] = "passed_offline_structure_and_references"
        write_json(stage / "manifest.json", manifest)
        stage.rename(destination)
    except BaseException:
        shutil.rmtree(stage)
        raise
    print(json.dumps({"output": str(destination), "dataset_id": digest, "counts": counts}, indent=2), flush=True)
    return manifest


def binary_rows(path, manifest):
    with path.open("rb") as stream:
        header = stream.read(HEADER.size)
        require(len(header) == HEADER.size, f"Truncated header: {path.name}")
        magic, version, header_size, record_size, count, start, digest = HEADER.unpack(header)
        require((magic, version, header_size, record_size) == (MAGIC, 1, 64, 64), "Invalid binary header")
        require(start == int(manifest["start_ns"]) and digest.hex() == manifest["dataset_id"], "Dataset identity mismatch")
        require(path.stat().st_size == HEADER.size + count * RECORD.size, "Truncated or trailing binary records")
        for _ in range(count):
            data = stream.read(RECORD.size)
            require(data[58:] == b"\0" * 6, "Nonzero reserved record bytes")
            yield RECORD.unpack(data)


def validate_bundle(directory):
    """Read the materialized binary independently; validate the Q input contract."""
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    require(manifest["format"] == "midas-normalized-order-flow" and manifest["version"] == 1, "Unknown format")
    for name, info in manifest["files"].items():
        path = directory / name
        require(path.stat().st_size == info["bytes"] and sha256(path) == info["sha256"], f"Checksum mismatch: {name}")
    orders, submissions, counters, operations, quantities = {}, {}, Counter(), Counter(), Counter()
    expected_sequence, previous_offset = 1, 0
    start, duration = int(manifest["start_ns"]), int(manifest["duration_ns"])
    counts = {}
    with gzip.open(directory / "commands.jsonl.gz", "rt", encoding="utf-8") as readable:
        for phase, name in (("bootstrap", "bootstrap.bin"), ("replay", "events.bin")):
            count = 0
            for values in binary_rows(directory / name, manifest):
                seq, time, offset, oid, price, quantity, shard, symbol, actor, op, side, tif, flags = values
                mirror = json.loads(next(readable))
                require(mirror.pop("phase") == phase and tuple(int(mirror[k]) for k in FIELDS) == values,
                        f"Readable/binary mismatch at {seq}")
                require(seq == expected_sequence, "Input sequence gap")
                expected_sequence += 1
                require(shard in (1, 2) and symbol == shard and actor == (symbol - 1) * 2 + side,
                        "Routing/actor mismatch")
                require(side in (BUY, SELL) and tif in (GTC, IOC) and flags & ~31 == 0, "Unknown enum/flag")
                require(oid >> 48 == shard and oid & Q_BIT and oid & ORDINAL_LIMIT, "Invalid Q order ID")
                require(start <= time < start + duration, "Event time outside window")
                if phase == "bootstrap":
                    require(op == NEW and flags & BOOTSTRAP and offset == 0 and tif == GTC, "Invalid bootstrap")
                else:
                    require(not flags & BOOTSTRAP and previous_offset <= offset < duration, "Invalid replay schedule")
                    require(offset >= time - start and (offset == time - start or flags & TIME_CLAMPED), "Invalid timing adjustment")
                    previous_offset = offset
                if op == NEW:
                    require(oid not in orders and price > 0 and price % 1_000_000 == 0 and quantity > 0, "Invalid new")
                    counters[shard] += 1
                    require(oid == order_id(shard, counters[shard]), "Q ordinal gap or reuse")
                    orders[oid] = (shard, symbol, actor, side)
                    submissions[oid] = (seq, price, quantity, tif)
                else:
                    require(oid in orders and orders[oid] == (shard, symbol, actor, side), "Invalid order reference/owner")
                    require(price == 0 and tif == GTC and ((op == CANCEL and quantity == 0) or
                                                          (op == REDUCE and quantity > 0)), "Invalid mutation")
                if phase == "replay":
                    operations[{NEW: "NEW", CANCEL: "CANCEL", REDUCE: "REDUCE_BY"}[op]] += 1
                    quantities[f"{symbol}:{op}"] += quantity
                count += 1
            counts[phase] = count
        require(not readable.read(), "Extra readable commands")
    require(counts["bootstrap"] == manifest["counts"]["bootstrap"] and counts["replay"] == manifest["counts"]["replay"],
            "Manifest count mismatch")
    require(dict(operations) == manifest["counts"]["operations"], "Manifest operation count mismatch")
    require({str(k): v for k, v in counters.items()} == manifest["counts"]["q_id_high_water"],
            "Manifest Q ID high-water mismatch")
    registry = {}
    with gzip.open(directory / "orders.jsonl.gz", "rt", encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            oid = int(row["order_id"])
            require(oid in orders and oid not in registry, "Invalid order dictionary")
            require(row["first_input_seq"] == submissions[oid][0] and
                    (row["actor_id"], row["side"]) == orders[oid][2:], "Order dictionary ownership mismatch")
            registry[oid] = (row["symbol"], row["source_order_id"], row["generation"])
    require(len(registry) == len(orders), "Missing order dictionary entry")
    trades, historical_volume = {}, Counter()
    with gzip.open(directory / "reference_trades.jsonl.gz", "rt", encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            key = (row["symbol"], row["trade_id"])
            require(key not in trades and row["quantity_e8"] > 0, "Duplicate/invalid reference trade")
            trades[key] = row
            historical_volume[row["symbol"]] += row["quantity_e8"]
    referenced_trades = set()
    provenance_count = 0
    with gzip.open(directory / "provenance.jsonl.gz", "rt", encoding="utf-8") as stream:
        for provenance_count, line in enumerate(stream, 1):
            row = json.loads(line)
            require(row["input_seq"] == provenance_count, "Provenance sequence mismatch")
            require(registry[int(row["order_id"])] == (row["symbol"], row["source_order_id"], row["generation"]),
                    "Provenance order mismatch")
            if row["source_kind"] == "aggressor_reconstruction":
                oid = int(row["order_id"])
                require(submissions[oid][0] == row["input_seq"], "Aggressor evidence is not attached to a new order")
                observed = 0
                for trade_id in row["trade_ids"]:
                    key = row["symbol"], trade_id
                    require(key in trades and key not in referenced_trades, "Unknown or double-counted historical fill")
                    referenced_trades.add(key)
                    trade = trades[key]
                    require(trade["taker_source_order_id"] == row["source_order_id"] and
                            trade["taker_side"] == orders[oid][3], "Historical taker association mismatch")
                    price = submissions[oid][1]
                    require(price >= trade["price_e8"] if trade["taker_side"] == BUY else price <= trade["price_e8"],
                            "Aggressor limit cannot reach its reference executions")
                    observed += trade["quantity_e8"]
                require(observed == row["observed_fill_quantity_e8"], "Aggressor observed volume mismatch")
                require(submissions[oid][2] >= observed + row["observed_resting_quantity_e8"],
                        "Aggressor quantity omits executions or its resting remainder")
    require(provenance_count == expected_sequence - 1, "Missing provenance")
    require(len(referenced_trades) == len(trades), "Historical executions omitted from reconstruction")
    for symbol, summary in manifest["per_symbol"].items():
        source = summary["counts"]
        require(sum(k[0] == symbol for k in trades) == source["unique_trades"] == source["fill_decrements_suppressed"],
                "Historical trade/decrement coverage mismatch")
        classified = (source.get("diff_new", 0) + source["fill_decrements_suppressed"] +
                      source.get("nontrade_cancels", 0) + source.get("nontrade_reductions", 0) +
                      source.get("zero_size_or_noop_outcomes_suppressed", 0) + source.get("cancel_replace_pairs", 0))
        require(classified == sum(source.get(f"diff_{kind}", 0) for kind in ("new", "update", "remove")),
                "Some source book changes were not classified")
    return {"passed": True, "scope": "offline_binary_timing_references_and_audit_integrity",
            "counts": counts, "unique_order_ids": len(orders), "operations": dict(operations),
            "last_offset_ns": str(previous_offset), "quantity_totals_e8": dict(quantities),
            "reference_trades_accounted_exactly_once": len(referenced_trades),
            "historical_executed_quantity_e8": dict(historical_volume),
            "all_source_book_changes_classified": True,
            "runtime_matching_tested": False, "runtime_durability_tested": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, nargs="?")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--validate", type=Path, metavar="BUNDLE")
    args = parser.parse_args()
    if args.validate:
        require(args.source is None and args.output is None, "Use --validate on its own")
        print(json.dumps(validate_bundle(args.validate), indent=2))
    else:
        require(args.source is not None and args.output is not None, "Specify source and --output")
        write_bundle(args.source, args.output)


if __name__ == "__main__":
    main()
