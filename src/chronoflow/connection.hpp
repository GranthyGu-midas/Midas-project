#pragma once
#include "shared_data/orders/engine_link.hpp"
#include "shared_data/orders/config.hpp"
#include "shared_data/runtime/io.hpp"

#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace midas::chronoflow {
class ConnectionLost : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class ReplayError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

class Interrupted : public std::runtime_error {
 public:
  Interrupted() : std::runtime_error("ChronoFlow interrupted") {
  }
};

// One sender and one control-reader. No business-result queue or per-order
// pending map exists. The writer mutex only serializes heartbeats with sends.
class Connection {
 public:
  Connection(const marketlab::orders::OrderConfig&, const std::atomic_bool&);
  ~Connection();
  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  void check();
  void send(const marketlab::orders::ReplayRequest&, std::uint32_t user);
  marketlab::orders::ReplayReady control(const marketlab::orders::ReplayRequest&,
                                         std::uint32_t user);

 private:
  const marketlab::orders::OrderConfig& config_;
  const std::atomic_bool& stop_;
  marketlab::runtime::Fd socket_;
  const std::uint64_t session_;
  std::uint64_t next_token_ = 1;
  std::atomic_bool closing_{false};
  std::atomic_bool lost_{false};
  std::mutex write_mutex_;
  std::mutex state_mutex_;
  std::condition_variable changed_;
  std::string failure_;
  bool fatal_ = false;
  std::unordered_map<std::uint64_t, marketlab::orders::ReplayReady> markers_;
  std::thread reader_;

  void write(marketlab::orders::LinkKind, std::uint64_t token, std::uint32_t user,
             std::span<const std::uint8_t>);
  marketlab::orders::Bytes frame();
  void receive();
  void fail(const std::string&, bool fatal);
};
}  // namespace midas::chronoflow
