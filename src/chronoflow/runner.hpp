#pragma once
#include "chronoflow/bundle.hpp"
#include <atomic>

namespace midas::chronoflow {
struct RunOptions {
  std::filesystem::path output = "build/logs/chronoflow";
  double speed = 1.0;
  std::uint32_t bootstrap_batch = 1024;
  std::uint32_t spin_us = 50;
  std::uint64_t max_lateness_us = 0;
  std::uint32_t recovery_seconds = 300;
};

int run(const Bundle&, const marketlab::orders::OrderConfig&, const RunOptions&,
        const std::atomic_bool& stop);
}  // namespace midas::chronoflow
