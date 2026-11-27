#include "chronoflow/connection.hpp"

#include <array>
#include <cstring>
#include <sys/socket.h>

namespace midas::chronoflow {
using namespace marketlab::orders;
namespace runtime = marketlab::runtime;

Connection::Connection(const OrderConfig& config, const std::atomic_bool& stop)
    : config_(config), stop_(stop), session_(random_token_seed()) {
  try {
    socket_ = runtime::connect_tcp(config.router_listen.host, config.router_listen.port,
                                   runtime::Clock::now() + std::chrono::seconds(2));
    write(LinkKind::Hello, 0, 0, link_version);
    const auto bytes = frame();
    if (!valid_link_record(bytes) || decode_link_header(bytes).kind != LinkKind::Ready)
      throw ReplayError("Router handshake/version mismatch");
  } catch (const ReplayError&) {
    throw;
  } catch (const std::exception& error) {
    throw ConnectionLost(error.what());
  }
  reader_ = std::thread([this] {
    receive();
  });
}

Connection::~Connection() {
  closing_.store(true);
  if (socket_)
    shutdown(socket_.get(), SHUT_RDWR);
  changed_.notify_all();
  if (reader_.joinable())
    reader_.join();
}

void Connection::write(LinkKind kind, std::uint64_t token, std::uint32_t user,
                       std::span<const std::uint8_t> payload) {
  std::array<std::uint8_t, max_link_frame> bytes{};
  const auto length = link_header_size + payload.size();
  if (length > bytes.size())
    throw ReplayError("Replay frame exceeds internal protocol limit");
  const auto session = user ? session_ : 0;
  const WireLinkHeader header{static_cast<std::uint32_t>(length), kind, token, user, session};
  std::memcpy(bytes.data(), &header, sizeof(header));
  if (!payload.empty())
    std::memcpy(bytes.data() + sizeof(header), payload.data(), payload.size());
  std::lock_guard lock(write_mutex_);
  runtime::send_all(socket_.get(), std::span(bytes).first(length),
                    runtime::Clock::now() + std::chrono::milliseconds(config_.request_timeout_ms));
}

Bytes Connection::frame() {
  const auto deadline = runtime::Clock::now() + std::chrono::milliseconds(config_.link_timeout_ms);
  const auto prefix = runtime::receive_exact(socket_.get(), 4, deadline);
  std::uint32_t length{};
  std::memcpy(&length, prefix.data(), sizeof(length));
  if (length < link_header_size || length > max_link_frame)
    throw ReplayError("Router sent an invalid frame length");
  auto bytes = runtime::receive_exact(socket_.get(), length - 4, deadline);
  bytes.insert(bytes.begin(), prefix.begin(), prefix.end());
  return bytes;
}

void Connection::fail(const std::string& message, bool fatal) {
  {
    std::lock_guard lock(state_mutex_);
    failure_ = message;
    fatal_ = fatal;
    lost_.store(true, std::memory_order_release);
  }
  changed_.notify_all();
  shutdown(socket_.get(), SHUT_RDWR);
}

void Connection::receive() {
  try {
    while (!closing_.load() && !stop_.load()) {
      const auto bytes = frame();
      if (!valid_link_record(bytes))
        throw ReplayError("Router sent an invalid control frame");
      const auto header = decode_link_header(bytes);
      if (header.kind == LinkKind::Ping) {
        write(LinkKind::Pong, 0, 0, {});
      } else if (header.kind == LinkKind::Pong) {
        continue;
      } else if (header.kind == LinkKind::MaintenanceBegin) {
        throw ConnectionLost("Planned maintenance: pause and recover durable input positions");
      } else if (header.kind == LinkKind::MaintenanceEnd) {
        continue;
      } else if (header.kind == LinkKind::ReplayUnavailable) {
        const auto shard = copy_record<std::uint16_t>(std::span(bytes).subspan(link_header_size));
        throw ConnectionLost("matching_engine_unavailable shard=" + std::to_string(shard) +
                             " (shutdown or link loss); waiting for durable recovery");
      } else if (header.kind == LinkKind::ReplayReady) {
        if (header.session != session_)
          throw ReplayError("Replay control response has the wrong session");
        const auto ready = copy_record<ReplayReady>(std::span(bytes).subspan(link_header_size));
        {
          std::lock_guard lock(state_mutex_);
          if (markers_.size() >= 16 || !markers_.emplace(header.token, ready).second)
            throw ReplayError("Duplicate or unsolicited replay control marker");
        }
        changed_.notify_all();
      } else {
        throw ReplayError(
            "ChronoFlow received a business result; one-way ingress contract violated");
      }
    }
  } catch (const ReplayError& error) {
    if (!closing_.load())
      fail(error.what(), true);
  } catch (const std::exception& error) {
    if (!closing_.load())
      fail(error.what(), false);
  }
}

void Connection::check() {
  if (stop_.load())
    throw Interrupted();
  if (lost_.load(std::memory_order_acquire)) {
    std::lock_guard lock(state_mutex_);
    if (fatal_)
      throw ReplayError(failure_);
    throw ConnectionLost(failure_);
  }
}

void Connection::send(const ReplayRequest& request, std::uint32_t user) {
  check();
  try {
    write(LinkKind::Replay, 0, user,
          {reinterpret_cast<const std::uint8_t*>(&request), sizeof(request)});
  } catch (const std::exception& error) {
    check();
    throw ConnectionLost(error.what());
  }
}

ReplayReady Connection::control(const ReplayRequest& request, std::uint32_t user) {
  check();
  const auto token = next_token_++;
  if (!token)
    throw ReplayError("Control token space exhausted");
  try {
    write(LinkKind::Replay, token, user,
          {reinterpret_cast<const std::uint8_t*>(&request), sizeof(request)});
  } catch (const std::exception& error) {
    throw ConnectionLost(error.what());
  }
  const auto deadline =
      runtime::Clock::now() + std::chrono::milliseconds(config_.request_timeout_ms);
  for (;;) {
    check();
    std::unique_lock lock(state_mutex_);
    const auto found = markers_.find(token);
    if (found != markers_.end()) {
      const auto marker = found->second;
      markers_.erase(found);
      if (marker.status || !marker.committed_seq || marker.dataset != request.dataset ||
          marker.shard != request.shard || marker.operation != request.operation)
        throw ReplayError(
            "Replay dataset/control mismatch; use the matching bundle and engine data directory");
      return marker;
    }
    if (runtime::Clock::now() >= deadline)
      throw ConnectionLost("Durable control marker timed out");
    changed_.wait_until(lock,
                        std::min(deadline, runtime::Clock::now() + std::chrono::milliseconds(50)));
  }
}
}  // namespace midas::chronoflow
