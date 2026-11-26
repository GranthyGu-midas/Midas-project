# Coinbase order semantics regression

Build with `-DMARKETLAB_BUILD_TESTS=ON`; SQLite is not required.

```sh
cmake -S . -B build/cmake -DMARKETLAB_BUILD_TESTS=ON -DMARKETLAB_BUILD_SQLITE_BENCHMARK=OFF
cmake --build build/cmake --parallel 4
ctest --test-dir build/cmake -R 'coinbase_semantics|coinbase_process' --output-on-failure
ctest --test-dir build/cmake --output-on-failure
```

For WSL with the source checkout on `/mnt/c`, put test data on a Linux filesystem:

```sh
cmake -S . -B build/cmake -DMARKETLAB_TEST_OUTPUT_DIR=/tmp/midas-test-results
ctest --test-dir build/cmake --output-on-failure
```

This changes only artifact paths. Test loads, timeouts and assertions stay the same. The sustained-load restart regression exceeded its 240-second deadline on the Windows-mounted data directory in this environment; the same test passed using Linux temporary storage. These functional checks do not establish production latency or disk-durability performance; `/tmp` may be memory-backed.

`coinbase_semantics` checks 10,000 seeded decimal round trips, precision/range/step rejection, uint64 level overflow without mutation, removal of legacy price limits, v3 market framing, FIFO after partial fills/reductions, no-op/ended/foreign/zero/too-small/too-large reduction requests, IOC for both sides with zero/partial/full execution, and all three relative sizes for dc. Persistence scenarios exercise snapshots followed by reductions, repeated reductions, STP and IOC, specification mismatch, crashes before order write/after order sync/before notification, and A/B replication followed by durable promotion. It uses independent expected fills and conserved quantities and opens no sockets.

`coinbase_process` uses an independently packed Python client and the actual binaries with BTC/ETH on two shards. It checks instrument discovery, exact decimal order/reduction/market data fields, IOC and dc, FIFO across a planned restart, a SIGKILL followed by cold recovery of reduced quantities, the actual console commands and validation, and twelve shared UDP batches at approximately 200ms. It needs local TCP/UDP permissions. Each run creates isolated temporary ports and a new output directory; processes are cleaned up and logs/data/results are retained for inspection.

The pre-existing engine/gateway/publisher/recovery/history/replication/client suites are retained and adapted for v3. Normal matching workloads use distinct buyer/seller accounts; STP is explicitly tested and must create no trade-history rows. Publisher loss recovery tests accept any later recovered batch because its freshness timeout spans several 200ms updates.

The sustained-load planned-restart suite also checks catch-up with an asynchronous trade-history index. Near the cutover threshold, the supervisor allows the index to drain to a fixed committed boundary before checking readiness again; log, FIFO, history and final promotion-boundary checks remain required.
