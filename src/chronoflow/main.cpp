#include "chronoflow/runner.hpp"

#include <csignal>
#include <iostream>
#include <limits>

#include "shared_data/paths.hpp"
#include "shared_data/runtime/thread.hpp"

namespace {
volatile std::sig_atomic_t interrupted = 0;

void on_signal(int) {
  interrupted = 1;
}

std::uint64_t integer(const std::string& text) {
  if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos)
    throw std::runtime_error("Expected a nonnegative integer");
  const auto value = std::stoull(text);
  if (value > std::numeric_limits<std::uint32_t>::max())
    throw std::runtime_error("Option exceeds uint32 range");
  return value;
}
}  // namespace

int main(int argc, char** argv) {
  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  std::signal(SIGPIPE, SIG_IGN);
  try {
    midas::chronoflow::RunOptions options;
    std::filesystem::path bundle_path;
    std::string config_path = marketlab::default_exchange_config;
    bool verify = false;
    for (int i = 1; i < argc; ++i) {
      const std::string flag = argv[i];
      if (flag == "--help") {
        std::cout
            << "ChronoFlow: historical order-flow replay\n"
               "market_chronoflow --bundle PATH [options]\n"
               "  --config PATH            Exchange config with replay actor accounts\n"
               "  --output PATH            Run reports and timing/recovery CSV files\n"
               "  --speed FACTOR           Historical clock speed; default 1\n"
               "  --bootstrap-batch N      Initialization records per durable barrier; default "
               "1024\n"
               "  --spin-us N              Busy-wait tail before deadlines; default 50\n"
               "  --max-lateness-us N      Abort beyond this lateness; 0 records without aborting\n"
               "  --recovery-seconds N     Reconnect deadline per outage; default 300\n"
               "  --verify-only            Verify bundle/config without connecting\n";
        return 0;
      }
      if (flag == "--verify-only") {
        verify = true;
        continue;
      }
      if (++i == argc)
        throw std::runtime_error("Missing option value");
      const std::string value = argv[i];
      if (flag == "--bundle")
        bundle_path = value;
      else if (flag == "--config")
        config_path = value;
      else if (flag == "--output")
        options.output = value;
      else if (flag == "--speed") {
        std::size_t consumed = 0;
        options.speed = std::stod(value, &consumed);
        if (consumed != value.size())
          throw std::runtime_error("Invalid speed");
      } else if (flag == "--bootstrap-batch")
        options.bootstrap_batch = integer(value);
      else if (flag == "--spin-us")
        options.spin_us = integer(value);
      else if (flag == "--max-lateness-us")
        options.max_lateness_us = integer(value);
      else if (flag == "--recovery-seconds")
        options.recovery_seconds = integer(value);
      else
        throw std::runtime_error("Unknown option: " + flag);
    }
    if (bundle_path.empty())
      throw std::runtime_error("--bundle PATH is required");
    const auto config = marketlab::orders::load_order_config(config_path);
    if (config.direct)
      throw std::runtime_error("ChronoFlow requires routing=router");
    const midas::chronoflow::Bundle bundle(bundle_path, config);
    std::cout << "CHRONOFLOW_BUNDLE_READY dataset=" << bundle.id
              << " bootstrap=" << bundle.bootstrap->header.count
              << " events=" << bundle.events->header.count << std::endl;
    if (verify)
      return 0;
    std::atomic_bool stop{false};
    marketlab::runtime::StopThread signals([&](const std::atomic_bool& cancelled) {
      while (!cancelled.load() && !interrupted)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (interrupted)
        stop.store(true);
    });
    return midas::chronoflow::run(bundle, config, options, stop);
  } catch (const std::exception& error) {
    std::cerr << "CHRONOFLOW_ERROR reason=" << error.what() << '\n';
    return 1;
  }
}
