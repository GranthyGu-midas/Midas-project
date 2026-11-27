#!/usr/bin/env python3
"""ChronoFlow/Gateway coexistence and recovery against two real worker processes."""
import argparse
import csv
import hashlib
import importlib.util
import json
from pathlib import Path
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("coinbase_process_helpers", ROOT / "tests/coinbase/process.py")
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
HEADER = struct.Struct("<8sHHIQQ32s")
RECORD = struct.Struct("<QQQQqqHHHBBBB6x")
SCALE = 100000000


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def eventually(predicate, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (OSError, EOFError):
            pass
        time.sleep(0.03)
    raise AssertionError("Process readiness/progress timed out")


def fixture(path):
    path.mkdir()
    start = 1795442100000000000
    duration = 2300000000
    digest = hashlib.sha256(b"ChronoFlow recovery fixture v1").digest()
    bootstrap = []
    events = []
    sequence = 0
    for shard in (1, 2):
        for side in (1, 2):
            sequence += 1
            oid = (shard << 48) | (1 << 47) | side
            actor = (shard - 1) * 2 + side
            price = (99 if side == 1 else 101) * SCALE
            bootstrap.append((sequence, start, 0, oid, price, 100 * SCALE,
                              shard, shard, actor, 1, side, 0, 1))
    for step in range(20):
        offset = (step + 1) * 100000000
        for shard in (1, 2):
            side = 1 if shard == 1 else 2
            actor = (shard - 1) * 2 + side
            sequence += 1
            oid = (shard << 48) | (1 << 47) | (step + 3)
            events.append((sequence, start + offset, offset, oid,
                           (101 if side == 1 else 99) * SCALE, SCALE,
                           shard, shard, actor, 1, side, 1, 0))
            sequence += 1
            oid = (shard << 48) | (1 << 47) | side
            events.append((sequence, start + offset, offset, oid, 0, SCALE // 10,
                           shard, shard, actor, 3, side, 0, 0))
    files = {}
    for name, records in (("bootstrap.bin", bootstrap), ("events.bin", events)):
        data = HEADER.pack(b"MIDASQ01", 1, 64, 64, len(records), start, digest)
        data += b"".join(RECORD.pack(*row) for row in records)
        (path / name).write_bytes(data)
        files[name] = dict(bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
    manifest = dict(format="midas-normalized-order-flow", version=1, numeric_scale=SCALE,
                    dataset_id=digest.hex(), start_ns=str(start), end_ns=str(start + duration),
                    duration_ns=str(duration), files=files,
                    counts=dict(bootstrap=len(bootstrap), replay=len(events)),
                    products=[dict(symbol=symbol, symbol_id=shard, shard_id=shard,
                                   price_tick_e8=1000000, quantity_step_e8=1)
                              for shard, symbol in ((1, "BTC"), (2, "ETH"))],
                    actors=[dict(actor_id=(shard - 1) * 2 + side,
                                 name=f"q_{symbol.lower()}_{label}", symbol=symbol, side=side)
                            for shard, symbol in ((1, "BTC"), (2, "ETH"))
                            for side, label in ((1, "buy"), (2, "sell"))])
    (path / "manifest.json").write_text(json.dumps(manifest))
    return manifest


class Deployment:
    def __init__(self, binary, root, manifest, launcher=False):
        self.binary = binary
        self.root = root
        self.root.mkdir()
        self.processes = []
        self.streams = []
        self.workers = {}
        self.manifest = manifest
        self.launcher = launcher
        self.generation = 0
        reserved = [socket.socket() for _ in range(4)]
        for connection in reserved:
            connection.bind(("127.0.0.1", 0))
        self.gateway, self.router, first, second = [s.getsockname()[1] for s in reserved]
        lines = ["routing=router", f"gateway_listen=127.0.0.1,{self.gateway}",
                 f"router_listen=127.0.0.1,{self.router}", f"engine_endpoint=127.0.0.1,{self.router}",
                 "user=Alice,test", "user=Bob,test"]
        lines += [f"user={actor['name']},test" for actor in manifest["actors"]]
        for product, port in zip(manifest["products"], (first, second)):
            shard, symbol = product["shard_id"], product["symbol"]
            lines += [f"shard={shard},127.0.0.1,{port}", f"symbol={symbol},100,{shard}",
                      f"instrument={symbol},0.01,0.00000001"]
        lines += ["request_timeout_ms=15000", "max_buffer_bytes=16777216"]
        self.config = root / "exchange.conf"
        self.config.write_text("\n".join(lines) + "\n")
        for connection in reserved:
            connection.close()
        try:
            for shard in (1, 2):
                self.worker(shard)
            self.launch([binary / "market_engine_router", "--config", self.config], "router.log")
            self.launch([binary / "market_gateway", "gateway", "--config", self.config], "gateway.log")
            eventually(self.ready)
        except BaseException:
            self.close()
            raise

    def launch(self, args, name):
        stream = (self.root / name).open("w")
        self.streams.append(stream)
        process = subprocess.Popen(list(map(str, args)), stdout=stream, stderr=subprocess.STDOUT)
        self.processes.append(process)
        return process

    def worker(self, shard):
        self.generation += 1
        process = self.launch([self.binary / "market_matching_engine", "--shard", shard,
                               "--config", self.config, "--data-dir", self.root / "engine-data",
                               "--checkpoint-ms", "10000" if int(self.manifest["duration_ns"]) > 10000000000 else "200"],
                              f"worker-{shard}-{self.generation}.log")
        self.workers[shard] = process
        return process

    def ready(self):
        client = helpers.Client(self.gateway, "Alice")
        client.close()
        return True

    def replay(self, bundle, speed, suffix=""):
        name = f"chronoflow{suffix}.log"
        if self.launcher:
            args = ["sh", ROOT / "src/scripts/run_chronoflow.sh", "--build-dir", self.binary,
                    "--log-dir", self.root / "reports", "--color", "always"]
        else:
            args = [self.binary / "market_chronoflow", "--output", self.root / "reports"]
        args += ["--bundle", bundle, "--config", self.config,
                 "--speed", speed, "--recovery-seconds", "30"]
        process = self.launch(args, name)
        return process, self.root / name

    def snapshots(self):
        result = {}
        for actor in self.manifest["actors"]:
            client = helpers.Client.__new__(helpers.Client)
            client.sock = socket.create_connection(("127.0.0.1", self.gateway), timeout=10)
            client.user = actor["name"]
            client.sequence = 0
            client.events = []
            try:
                result[client.user] = client.snapshot()
            finally:
                client.close()
        return result

    def close(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for stream in self.streams:
            stream.close()


def exercise(binary, root, bundle, manifest, mode, speed=1, real=False, launcher=False):
    deployment = Deployment(binary, root, manifest, launcher)
    client = None
    try:
        client = helpers.Client(deployment.gateway, "Alice")
        process, log = deployment.replay(bundle, speed)
        eventually(lambda: "CHRONOFLOW_REPLAY_STARTED" in log.read_text(), timeout=120)
        # The client predates Q: this round trip fails if Q replaced the Gateway's
        # router connection. A distant native order also exercises separate IDs.
        oid = client.order("BTC", 1, "1", "0.1")
        check(not oid & (1 << 47), "Gateway order used the replay ID namespace")
        client.history()
        client.close()
        client = None
        if mode == "worker-crash":
            time.sleep(0.6 if not real else 2)
            deployment.workers[1].kill()
            deployment.workers[1].wait(timeout=10)
            time.sleep(0.3)
            deployment.worker(1)
        elif mode == "producer-restart":
            time.sleep(0.6)
            process.terminate()
            check(process.wait(timeout=15) == (143 if launcher else 130), "ChronoFlow did not stop cleanly")
            process, log = deployment.replay(bundle, speed, "-resumed")
        timeout = max(30, int(int(manifest["duration_ns"]) / 1e9 / float(speed)) + 90)
        check(process.wait(timeout=timeout) == 0, f"ChronoFlow failed; inspect {log}")
        reports = sorted([json.loads(p.read_text()) for p in (root / "reports").rglob("*.json")],
                         key=lambda value: value["complete"])
        report = reports[-1]
        check(report["complete"] and report["business_results_received"] == 0, "Invalid completion/no-reply contract")
        for value in report["shards"].values():
            check(value["last_durable_input"] == value["expected_final_input"], "Incomplete durable input prefix")
        if mode == "worker-crash":
            check(report["connected_sessions"] >= 2, "Worker crash did not exercise replay recovery")
        markers = []
        for path in sorted((root / "reports").rglob("*.recovery.csv")):
            with path.open() as stream:
                markers.extend(csv.DictReader(stream))
        if mode != "baseline":
            check(any(int(x["control_operation"]) == 0 and int(x["durable_input_seq"]) > manifest["counts"]["bootstrap"]
                      for x in markers), "Resume did not use a persisted replay position")
        for path in (root / "reports").rglob("*.timing.csv"):
            anchors = {}
            with path.open() as stream:
                for row in csv.DictReader(stream):
                    if not int(row["send_end_monotonic_ns"]):
                        continue
                    planned = int(row["planned_monotonic_ns"])
                    check(int(row["send_begin_monotonic_ns"]) >= planned, "An event was sent early")
                    anchor = planned - int(int(row["offset_ns"]) / float(speed))
                    attempt = row["session_attempt"]
                    if attempt in anchors:
                        check(abs(anchor - anchors[attempt]) <= 1, "Replay clock drifted or shards used different clocks")
                    anchors[attempt] = anchor
        if launcher:
            terminal = log.read_text()
            services = list((root / "reports").rglob("service.log"))
            check(services and "\x1b[32m" in terminal, "Launcher did not color ready/completion output")
            for service in services:
                check("\x1b[" not in service.read_text(), "ANSI escapes leaked into persistent service log")
            if mode == "worker-crash":
                check("CHRONOFLOW_PAUSED" in terminal and "matching_engine_unavailable shard=1" in terminal,
                      "Launcher did not identify the unavailable matching engine")
                check("\x1b[31m" in terminal and "CHRONOFLOW_RESUMED" in terminal,
                      "Launcher did not distinguish outage and resumed output")
            check("CHRONOFLOW_COMPLETE" in terminal, "Launcher lost the final log line")
        result = deployment.snapshots()
        print(json.dumps(dict(mode=mode, result="PASS", root=str(root), report=report)), flush=True)
        return result
    finally:
        if client is not None:
            client.close()
        deployment.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--real-bundle", type=Path)
    parser.add_argument("--speed", default="1")
    parser.add_argument("--fault", action="store_true")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix="run-", dir=args.output))
    if args.real_bundle:
        manifest = json.loads((args.real_bundle / "manifest.json").read_text())
        exercise(args.binary, root / "real", args.real_bundle, manifest,
                 "worker-crash" if args.fault else "baseline", args.speed, real=True)
    else:
        bundle = root / "bundle"
        manifest = fixture(bundle)
        baseline = exercise(args.binary, root / "baseline", bundle, manifest, "baseline")
        for mode in ("worker-crash", "producer-restart"):
            result = exercise(args.binary, root / mode, bundle, manifest, mode)
            check(result == baseline, f"{mode} changed the recovered final books")
            launched = exercise(args.binary, root / ("launcher-" + mode), bundle, manifest, mode, launcher=True)
            check(launched == baseline, f"Launcher {mode} changed the recovered final books")
        print("PASS two-shard FIFO replay, Gateway coexistence, engine/Q restart, durable resume and timing", flush=True)


if __name__ == "__main__":
    main()
