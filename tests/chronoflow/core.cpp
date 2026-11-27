#include "matching_engine/persistence/durable_engine.hpp"
#include "chronoflow/json.hpp"
#include "chronoflow/sha256.hpp"

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <unistd.h>

namespace m = marketlab;
namespace o = marketlab::orders;
namespace p = marketlab::orders::persistence;
using namespace std::chrono_literals;

namespace {
constexpr auto scale = 100000000LL;

void check(bool condition, const char* message) {
  if (!condition)
    throw std::runtime_error(message);
}

o::OrderConfig config() {
  o::OrderConfig result;
  result.accounts = {{"strategy", "test"},
                     {"background_sell", "test"},
                     {"other_strategy", "test"},
                     {"background_buy", "test"}};
  result.products = {{"BTC", 100, 1, 1000000, 1}};
  result.shards = {{1, {"127.0.0.1", 1}}};
  return result;
}

o::DatasetId dataset() {
  o::DatasetId id{};
  id[0] = 7;
  return id;
}

std::uint64_t qid(std::uint64_t ordinal) {
  return (1ULL << 48) | o::replay_id_bit | ordinal;
}

struct Events {
  std::vector<p::Event> values;

  o::EngineBackend::Emit emit() {
    return [this](auto kind, const auto& header, auto bytes) {
      p::Event event{};
      event.kind = kind;
      event.context = header;
      event.size = bytes.size();
      std::copy(bytes.begin(), bytes.end(), event.payload.begin());
      values.push_back(event);
    };
  }

  std::vector<o::ReplayReady> controls() const {
    std::vector<o::ReplayReady> result;
    for (const auto& event : values)
      if (event.kind == o::LinkKind::ReplayReady)
        result.push_back(
            o::copy_record<o::ReplayReady>(std::span(event.payload).first(event.size)));
    return result;
  }
};

void submit(o::EngineBackend& engine, Events& events, o::ReplayOp operation,
            std::uint64_t sequence = 0, std::uint64_t id = 0, std::int64_t quantity = 0,
            unsigned user = 2, m::Side side = m::Side::Sell) {
  o::ReplayRequest request{};
  request.dataset = dataset();
  request.input_seq = sequence;
  request.order_id = id;
  request.event_time_ns = sequence ? 1000000 + sequence : 0;
  request.shard = 1;
  request.operation = operation;
  request.side = side;
  request.quantity = quantity * scale;
  request.price = operation == o::ReplayOp::New ? 100 * scale : 0;
  o::put_string(request.symbol, "BTC");
  const auto token =
      operation == o::ReplayOp::Begin || operation == o::ReplayOp::Barrier ? 991ULL : 0;
  engine.command({0, o::LinkKind::Replay, token, user, 7}, o::record_bytes(request), events.emit());
}

void native(o::EngineBackend& engine, Events& events, unsigned user, m::Side side, int quantity) {
  m::NewOrderRequest request{};
  request.header = {m::MessageType::NewOrderRequest, sizeof(request), 900};
  o::put_string(request.symbol, "BTC");
  request.side = side;
  request.price = 100 * scale;
  request.quantity = quantity * scale;
  engine.command({0, o::LinkKind::NewOrder, 900, user, 2}, o::record_bytes(request), events.emit());
}

std::vector<m::OrderState> active(o::EngineBackend& engine, unsigned user) {
  Events events;
  m::Header request{m::MessageType::ActiveOrderSnapshot, sizeof(request), 901};
  engine.command({0, o::LinkKind::Snapshot, 901, user, 2}, o::record_bytes(request), events.emit());
  engine.drain();
  std::vector<m::OrderState> result;
  for (const auto& event : events.values) {
    const auto record =
        o::copy_record<m::ActiveOrderSnapshot>(std::span(event.payload).first(event.size));
    if (!record.isEnd)
      result.push_back(record.order);
  }
  return result;
}

void fifo_and_recovery(const std::filesystem::path& directory, bool checkpoint) {
  p::Options options;
  options.directory = directory;
  options.checkpoint_interval = 1h;
  std::uint64_t strategy_id = 0;
  {
    p::DurableEngine engine(config(), 1, options);
    Events events;
    submit(engine, events, o::ReplayOp::Begin);
    engine.drain();
    events.values.clear();
    submit(engine, events, o::ReplayOp::New, 1, qid(1), 5);
    engine.drain();
    check(events.values.empty(), "Replay new order generated a per-order result");
    native(engine, events, 3, m::Side::Sell, 5);
    engine.drain();
    strategy_id = active(engine, 3).at(0).orderId;
    check(strategy_id < qid(1), "Test does not reverse ID/arrival ordering");
    events.values.clear();
    submit(engine, events, o::ReplayOp::ReduceBy, 2, qid(1), 2);
    engine.drain();
    check(events.values.empty(), "Replay reduction generated a per-order result");
    check(active(engine, 2).at(0).remainingQuantity == 3 * scale, "Delta reduction failed");
    if (checkpoint) {
      engine.request_checkpoint();
      engine.wait_checkpoint();
    }
  }
  {
    p::DurableEngine engine(config(), 1, options);
    Events events;
    submit(engine, events, o::ReplayOp::Begin);
    engine.drain();
    const auto ready = events.controls().at(0);
    check(ready.input_seq == 2 && ready.event_time_ns == 1000002 && ready.committed_seq,
          "Recovered input sequence/time was not returned");
    events.values.clear();
    // Duplicate input must not reduce the order twice after reconnecting.
    submit(engine, events, o::ReplayOp::ReduceBy, 2, qid(1), 2);
    native(engine, events, 1, m::Side::Buy, 4);
    engine.drain();
    check(active(engine, 2).empty(),
          "Recovered FIFO moved the earlier Q order behind the native order");
    const auto remaining = active(engine, 3).at(0);
    check(remaining.orderId == strategy_id && remaining.remainingQuantity == 4 * scale,
          "Recovered FIFO or partial-cancel state changed");
    for (const auto& event : events.values)
      check(event.context.user != 2 && event.context.user != 4,
            "A later maker fill generated a Q business result");
    events.values.clear();
    submit(engine, events, o::ReplayOp::New, 3, qid(2), 2, 4, m::Side::Buy);
    submit(engine, events, o::ReplayOp::ReduceBy, 4, qid(1), 1);
    submit(engine, events, o::ReplayOp::Barrier, 4);
    engine.drain();
    check(events.controls().at(0).input_seq == 4,
          "Failed mutation did not advance durable input progress");
    check(engine.boundary().replay.next_order == 3, "External ID high water was not retained");
  }
}

struct Gate {
  std::mutex mutex;
  std::condition_variable changed;
  bool reached = false;
  bool open = false;

  void wait() {
    std::unique_lock lock(mutex);
    reached = true;
    changed.notify_all();
    changed.wait(lock, [&] {
      return open;
    });
  }

  void await() {
    std::unique_lock lock(mutex);
    check(changed.wait_for(lock, 5s,
                           [&] {
                             return reached;
                           }),
          "Persistence gate was not reached");
  }

  void release() {
    std::lock_guard lock(mutex);
    open = true;
    changed.notify_all();
  }
};

struct Release {
  Gate& gate;
  ~Release() {
    gate.release();
  }
};

void durable_control_barrier(const std::filesystem::path& directory, bool trades) {
  Gate gate;
  std::atomic_bool armed = false;
  p::Options options;
  options.directory = directory;
  options.hook = [&](auto stage, auto) {
    if (armed.load() && stage == (trades ? p::Stage::BeforeTradeSync : p::Stage::BeforeOrderSync))
      gate.wait();
  };
  p::DurableEngine engine(config(), 1, options);
  Release release{gate};
  Events events;
  submit(engine, events, o::ReplayOp::Begin);
  engine.drain();
  if (trades) {
    submit(engine, events, o::ReplayOp::New, 1, qid(1), 5);
    engine.drain();
  }
  events.values.clear();
  armed.store(true);
  if (trades)
    submit(engine, events, o::ReplayOp::New, 2, qid(2), 1, 4, m::Side::Buy);
  else
    submit(engine, events, o::ReplayOp::New, 1, qid(1), 5);
  gate.await();
  submit(engine, events, o::ReplayOp::Cancel, 3, qid(1));
  submit(engine, events, o::ReplayOp::Barrier, 3);
  engine.poll();
  check(events.values.empty(), "Control marker escaped before both logs were synced");
  gate.release();
  engine.drain();
  check(events.values.size() == 1 && events.controls().at(0).input_seq == 3,
        "Expected exactly one durable control response and no order results");
  check(active(engine, 2).empty(), "Cancel before the new-order persistence reply failed");
}

void encoding_utilities() {
  using namespace midas::chronoflow;
  const std::string abc = "abc";
  check(sha256({reinterpret_cast<const std::uint8_t*>(abc.data()), abc.size()}) ==
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "SHA-256 standard vector mismatch");
  check(sha256({}) == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "SHA-256 empty vector mismatch");
  const auto value = JsonReader("{\"id\":\"18446744073709551615\",\"array\":[1,2]}").parse();
  check(value.at("id").number() == UINT64_MAX, "JSON rounded a 64-bit identifier");
  bool rejected = false;
  try {
    JsonReader("{\"id\":1,\"id\":2}").parse();
  } catch (const std::exception&) {
    rejected = true;
  }
  check(rejected, "Duplicate manifest keys were accepted");
}

void replication_resume(const std::filesystem::path& directory) {
  p::Options options;
  options.directory = directory;
  options.checkpoint_interval = 1h;
  o::MatchingEngine source(config(), 1);
  p::OrderDictionary dictionary(config());
  std::uint64_t batch = 0;
  {
    p::DurableEngine replica(config(), 1, options);
    const auto forward = [&](o::ReplayOp operation, std::uint64_t sequence, int quantity) {
      o::ReplayRequest request{};
      request.dataset = dataset();
      request.input_seq = sequence;
      request.order_id = sequence ? qid(1) : 0;
      request.event_time_ns = sequence ? 1000000 + sequence : 0;
      o::put_string(request.symbol, "BTC");
      request.shard = 1;
      request.operation = operation;
      request.side = m::Side::Sell;
      request.price = 100 * scale;
      request.quantity = quantity * scale;
      p::Result result;
      source.execute({0, o::LinkKind::Replay, sequence ? 0ULL : 1ULL, 2, 7},
                     o::record_bytes(request), result);
      p::Boundary boundary{++batch, source.next_order(), source.next_trade()};
      boundary.replay = source.replay_state();
      o::Bytes bytes;
      p::start_frame(bytes, p::Kind::Orders, boundary, 0, result.changes.size());
      for (const auto& change : result.changes) {
        p::integer(bytes, change.erase ? 0 : 1, 1);
        if (change.erase)
          p::integer(bytes, change.order.id, 8);
        else
          p::encode_order(bytes, dictionary.expand(change.order));
      }
      p::finish_frame(bytes);
      replica.replicate(p::decode_frame(bytes), std::nullopt);
      replica.drain();
      check(replica.boundary().replay == source.replay_state(), "Replication lost replay progress");
    };
    forward(o::ReplayOp::Begin, 0, 0);
    forward(o::ReplayOp::New, 1, 5);
    forward(o::ReplayOp::ReduceBy, 2, 2);
    replica.request_checkpoint();
    replica.wait_checkpoint();
  }
  p::DurableEngine recovered(config(), 1, options);
  Events events;
  submit(recovered, events, o::ReplayOp::Begin);
  recovered.drain();
  check(events.controls().at(0).input_seq == 2,
        "Promoted replica returned the wrong replay prefix");
  submit(recovered, events, o::ReplayOp::ReduceBy, 2, qid(1), 2);
  recovered.drain();
  check(active(recovered, 2).at(0).remainingQuantity == 3 * scale,
        "Promoted replica applied an already committed input twice");
  submit(recovered, events, o::ReplayOp::Cancel, 3, qid(1));
  recovered.drain();
  check(active(recovered, 2).empty(), "Promoted replica could not continue replay");
}
}  // namespace

int main(int argc, char** argv) {
  try {
    if (argc != 2)
      throw std::runtime_error("Expected output directory");
    const auto root = std::filesystem::path(argv[1]) / std::to_string(getpid());
    std::filesystem::create_directories(root);
    encoding_utilities();
    fifo_and_recovery(root / "wal", false);
    fifo_and_recovery(root / "checkpoint", true);
    durable_control_barrier(root / "order-gate", false);
    durable_control_barrier(root / "trade-gate", true);
    replication_resume(root / "replica");
    std::cout << "PASS ChronoFlow FIFO, recovery, one-way input, durable barriers and encoding\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "FAIL " << error.what() << '\n';
    return 1;
  }
}
