#include "chronoflow/runner.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iostream>
#include <thread>
#include <time.h>
#include <unistd.h>

#include "chronoflow/connection.hpp"

namespace midas::chronoflow {
namespace {
using namespace marketlab::orders;

std::uint64_t monotonic_ns() {
  timespec now{};
  if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
    throw ReplayError("Cannot read monotonic clock");
  return static_cast<std::uint64_t>(now.tv_sec) * 1000000000ULL + now.tv_nsec;
}

void wait_until(std::uint64_t deadline, std::uint64_t spin_ns, Connection& connection) {
  for (;;) {
    connection.check();
    const auto now = monotonic_ns();
    if (now >= deadline)
      return;
    if (deadline - now <= spin_ns) {
      std::atomic_signal_fence(std::memory_order_seq_cst);
      continue;
    }
    // Absolute waits avoid cumulative drift. Bound sleeps so failures/signals
    // interrupt the schedule promptly even across large historical gaps.
    const auto wake = std::min<std::uint64_t>(deadline - spin_ns, now + 50000000ULL);
    const timespec target{static_cast<time_t>(wake / 1000000000ULL),
                          static_cast<long>(wake % 1000000000ULL)};
    const auto error = clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &target, nullptr);
    if (error && error != EINTR)
      throw ReplayError("Absolute monotonic wait failed");
  }
}

struct Timing {
  std::uint64_t planned = 0;
  std::uint64_t begin = 0;
  std::uint64_t end = 0;
  std::uint64_t attempt = 0;
  std::uint64_t transmissions = 0;
};

struct RunState {
  std::vector<Timing> timings;
  std::vector<std::uint64_t> durable = std::vector<std::uint64_t>(65536, 0);
  std::vector<std::uint64_t> sent = std::vector<std::uint64_t>(65536, 0);
  std::uint64_t attempts = 0;
  std::uint64_t sessions = 0;
  std::uint64_t control_markers = 0;
  std::uint64_t bootstrap_transmissions = 0;
  std::uint64_t replay_transmissions = 0;
};

ReplayReady control(Connection& connection, const Bundle& bundle, std::uint16_t shard,
                    ReplayOp operation, std::uint64_t sequence, RunState& state,
                    std::ofstream& recovery) {
  ReplayRequest request{};
  request.dataset = bundle.dataset;
  request.shard = shard;
  request.operation = operation;
  request.input_seq = sequence;
  const auto marker = connection.control(request, bundle.control_user(shard));
  if (marker.input_seq) {
    const auto record = bundle.sequence(marker.input_seq);
    if (record.shard != shard || record.event_time_ns != marker.event_time_ns)
      throw ReplayError("Engine recovery sequence/time does not belong to this shard and bundle");
  } else if (marker.event_time_ns) {
    throw ReplayError("Empty recovery prefix has a nonempty event time");
  }
  if (operation == ReplayOp::Barrier && marker.input_seq != sequence)
    throw ReplayError("Engine durable barrier does not cover the requested prefix");
  state.durable[shard] = marker.input_seq;
  ++state.control_markers;
  recovery << unix_ns() << ',' << state.attempts << ',' << shard << ','
           << static_cast<unsigned>(operation) << ',' << marker.input_seq << ','
           << marker.event_time_ns << ',' << marker.committed_seq << '\n';
  recovery.flush();
  if (!recovery)
    throw ReplayError("Cannot write recovery control audit");
  return marker;
}

void bootstrap(Connection& connection, const Bundle& bundle, const RunOptions& options,
               RunState& state, std::ofstream& recovery) {
  std::vector<std::uint32_t> outstanding(65536, 0);
  for (std::uint64_t i = 0; i < bundle.bootstrap->header.count; ++i) {
    const auto row = bundle.bootstrap->at(i);
    if (row.input_seq <= state.durable[row.shard])
      continue;
    connection.send(bundle.request(row), bundle.actors[row.actor]);
    state.sent[row.shard] = row.input_seq;
    ++state.bootstrap_transmissions;
    if (++outstanding[row.shard] == options.bootstrap_batch) {
      control(connection, bundle, row.shard, ReplayOp::Barrier, state.sent[row.shard], state,
              recovery);
      outstanding[row.shard] = 0;
    }
  }
  for (auto shard : bundle.shards)
    control(connection, bundle, shard, ReplayOp::Barrier, state.sent[shard], state, recovery);
  std::cout << "CHRONOFLOW_BOOTSTRAP_READY sessions=" << state.sessions << std::endl;
}

void replay(Connection& connection, const Bundle& bundle, const RunOptions& options,
            RunState& state, std::ofstream& recovery) {
  std::uint64_t first = 0;
  while (first < bundle.events->header.count) {
    const auto row = bundle.events->at(first);
    if (row.input_seq > state.durable[row.shard])
      break;
    ++first;
  }
  bool resumed = false;
  for (auto shard : bundle.shards)
    resumed = resumed || state.durable[shard] > bundle.bootstrap_last[shard];
  const auto offset = first == bundle.events->header.count
                          ? bundle.duration_ns
                          : (resumed ? bundle.events->at(first).offset_ns : 0);
  const auto anchor = monotonic_ns() + 100000000ULL;
  const auto scaled = [&](std::uint64_t historical_ns) {
    return static_cast<std::uint64_t>(static_cast<long double>(historical_ns) / options.speed);
  };
  std::cout << "CHRONOFLOW_REPLAY_STARTED offset_ns=" << offset << " speed=" << options.speed
            << " session=" << state.sessions << std::endl;
  if (resumed)
    std::cout << "CHRONOFLOW_RESUMED source_offset_ns=" << offset
              << " durable_prefixes_verified=true" << std::endl;
  auto next_progress = monotonic_ns() + 30000000000ULL;
  for (std::uint64_t i = first; i < bundle.events->header.count; ++i) {
    const auto row = bundle.events->at(i);
    if (row.input_seq <= state.durable[row.shard])
      continue;
    const auto deadline = anchor + scaled(row.offset_ns - offset);
    wait_until(deadline, options.spin_us * 1000ULL, connection);
    auto& timing = state.timings[i];
    timing.planned = deadline;
    timing.begin = monotonic_ns();
    timing.end = 0;
    timing.attempt = state.attempts;
    ++timing.transmissions;
    if (options.max_lateness_us && timing.begin - deadline > options.max_lateness_us * 1000ULL)
      throw ReplayError("Replay exceeded --max-lateness-us; no input was silently skipped");
    connection.send(bundle.request(row), bundle.actors[row.actor]);
    timing.end = monotonic_ns();
    state.sent[row.shard] = row.input_seq;
    ++state.replay_transmissions;
    if (timing.end >= next_progress) {
      std::cout << "CHRONOFLOW_PROGRESS input=" << row.input_seq << " offset_ns=" << row.offset_ns
                << " send_lateness_ns=" << timing.end - deadline << std::endl;
      next_progress = timing.end + 30000000000ULL;
    }
  }
  wait_until(anchor + scaled(bundle.duration_ns - offset), options.spin_us * 1000ULL, connection);
  for (auto shard : bundle.shards) {
    control(connection, bundle, shard, ReplayOp::Barrier, state.sent[shard], state, recovery);
    if (state.durable[shard] != bundle.final_last[shard])
      throw ReplayError("Final durable prefix is short of the dataset");
  }
}

void write_report(const std::filesystem::path& base, const Bundle& bundle,
                  const RunOptions& options, const RunState& state, bool complete,
                  std::uint64_t elapsed_ns) {
  std::ofstream timing(base.string() + ".timing.csv");
  timing << "input_seq,shard,offset_ns,session_attempt,transmissions,planned_monotonic_ns,"
            "send_begin_monotonic_ns,send_end_monotonic_ns,lateness_ns\n";
  std::vector<std::uint64_t> delays;
  for (std::uint64_t i = 0; i < state.timings.size(); ++i) {
    const auto& sample = state.timings[i];
    if (!sample.transmissions)
      continue;
    const auto record = bundle.events->at(i);
    const auto delay = sample.end ? sample.end - sample.planned : 0;
    if (sample.end)
      delays.push_back(delay);
    timing << record.input_seq << ',' << record.shard << ',' << record.offset_ns << ','
           << sample.attempt << ',' << sample.transmissions << ',' << sample.planned << ','
           << sample.begin << ',' << sample.end << ',' << delay << '\n';
  }
  timing.close();
  if (!timing)
    throw ReplayError("Cannot write timing audit");
  std::sort(delays.begin(), delays.end());
  const auto percentile = [&](std::size_t percent) {
    return delays.empty() ? 0 : delays[(delays.size() - 1) * percent / 100];
  };
  std::ofstream report(base.string() + ".json");
  report << "{\n  \"component\": \"ChronoFlow\",\n  \"complete\": " << (complete ? "true" : "false")
         << ",\n  \"dataset_id\": \"" << bundle.id << "\",\n  \"speed\": " << options.speed
         << ",\n  \"elapsed_wall_ns\": " << elapsed_ns
         << ",\n  \"connection_attempts\": " << state.attempts
         << ",\n  \"connected_sessions\": " << state.sessions
         << ",\n  \"control_markers\": " << state.control_markers
         << ",\n  \"business_results_received\": 0"
         << ",\n  \"bootstrap_transmissions\": " << state.bootstrap_transmissions
         << ",\n  \"replay_transmissions\": " << state.replay_transmissions
         << ",\n  \"timing_observations\": " << delays.size()
         << ",\n  \"send_lateness_ns\": {\"p50\": " << percentile(50)
         << ", \"p99\": " << percentile(99) << ", \"max\": " << percentile(100)
         << "},\n  \"shards\": {";
  bool first = true;
  for (auto shard : bundle.shards) {
    report << (first ? "\n" : ",\n") << "    \"" << shard
           << "\": {\"last_sent_input\": " << state.sent[shard]
           << ", \"last_durable_input\": " << state.durable[shard]
           << ", \"expected_final_input\": " << bundle.final_last[shard] << '}';
    first = false;
  }
  report << "\n  }\n}\n";
  report.close();
  if (!report)
    throw ReplayError("Cannot write run report");
  std::cout << "CHRONOFLOW_REPORT path=" << base.string() + ".json" << std::endl;
}
}  // namespace

int run(const Bundle& bundle, const marketlab::orders::OrderConfig& config,
        const RunOptions& options, const std::atomic_bool& stop) {
  if (!std::isfinite(options.speed) || options.speed < 0.01 || options.speed > 1000 ||
      !options.bootstrap_batch || options.bootstrap_batch > 8192 || options.spin_us > 1000 ||
      !options.recovery_seconds)
    throw ReplayError("Invalid ChronoFlow speed/buffering/recovery options");
  std::filesystem::create_directories(options.output);
  const auto base =
      options.output / ("run-" + std::to_string(unix_ns()) + "-" + std::to_string(getpid()));
  std::ofstream recovery(base.string() + ".recovery.csv");
  if (!recovery)
    throw ReplayError("Cannot create recovery audit");
  recovery << "wall_time_ns,connection_attempt,shard,control_operation,durable_input_seq,"
              "historical_event_time_ns,commit_seq\n";
  RunState state;
  state.timings.resize(bundle.events->header.count);
  const auto started = monotonic_ns();
  auto recovery_deadline = started + options.recovery_seconds * 1000000000ULL;
  auto next_notice = started;
  bool complete = false;
  bool paused = true;
  int result = 0;
  std::exception_ptr failure;
  try {
    while (!complete) {
      if (stop.load())
        throw Interrupted();
      ++state.attempts;
      try {
        Connection connection(config, stop);
        ++state.sessions;
        for (auto shard : bundle.shards) {
          const auto marker =
              control(connection, bundle, shard, ReplayOp::Begin, 0, state, recovery);
          state.sent[shard] = marker.input_seq;
          std::cout << "CHRONOFLOW_RECOVERY_POINT shard=" << shard << " input=" << marker.input_seq
                    << " source_time_ns=" << marker.event_time_ns << std::endl;
        }
        bootstrap(connection, bundle, options, state, recovery);
        recovery_deadline = 0;
        paused = false;
        replay(connection, bundle, options, state, recovery);
        complete = true;
      } catch (const ConnectionLost& error) {
        const auto now = monotonic_ns();
        if (!paused) {
          std::cout << "CHRONOFLOW_PAUSED reason=" << error.what()
                    << "; market input paused until matching engines and Router recover"
                    << std::endl;
          paused = true;
        }
        if (!recovery_deadline)
          recovery_deadline = now + options.recovery_seconds * 1000000000ULL;
        if (now >= recovery_deadline)
          throw ReplayError("Recovery timeout: " + std::string(error.what()));
        if (now >= next_notice) {
          std::cout << "CHRONOFLOW_RECOVERY_WAIT reason=" << error.what() << std::endl;
          next_notice = now + 1000000000ULL;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(config.reconnect_ms));
      }
    }
  } catch (const Interrupted&) {
    result = 130;
  } catch (...) {
    failure = std::current_exception();
  }
  write_report(base, bundle, options, state, complete, monotonic_ns() - started);
  if (failure)
    std::rethrow_exception(failure);
  std::cout << (complete ? "CHRONOFLOW_COMPLETE" : "CHRONOFLOW_STOPPED") << std::endl;
  return result;
}
}  // namespace midas::chronoflow
