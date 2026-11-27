#pragma once
#include "shared_data/orders/protocol.h"
#include <algorithm>
#include <array>

namespace marketlab::orders {
using DatasetId = std::array<std::uint8_t, 32>;
inline constexpr std::uint64_t replay_id_bit = 1ULL << 47;
inline constexpr std::uint64_t order_ordinal_mask = replay_id_bit - 1;
inline bool empty_dataset(const DatasetId& id) {
  return std::all_of(id.begin(), id.end(), [](auto b) {
    return b == 0;
  });
}
struct ReplayState {
  std::uint64_t next_order = 1;
  std::uint64_t last_input = 0;
  std::uint64_t last_event_ns = 0;
  DatasetId dataset{};
  bool operator==(const ReplayState&) const = default;
};
inline bool replay_progresses(const ReplayState& before, const ReplayState& after) {
  return after.next_order >= before.next_order && after.last_input >= before.last_input &&
         (after.last_input != before.last_input ||
          (after.last_event_ns == before.last_event_ns && after.next_order == before.next_order)) &&
         (empty_dataset(before.dataset) || before.dataset == after.dataset);
}
inline bool valid_order_id(OrderId id, std::uint16_t shard, std::uint64_t next_native,
                           const ReplayState& replay) {
  const auto ordinal = id & order_ordinal_mask;
  return (id >> 48) == shard && ordinal &&
         ordinal < ((id & replay_id_bit) ? replay.next_order : next_native);
}
enum class ReplayOp : std::uint8_t { Begin = 0, New = 1, Cancel = 2, ReduceBy = 3, Barrier = 4 };

#pragma pack(push, 1)
// Internal trusted ingress, independent of the public gateway request format.
// Data commands have envelope token=0 and never produce a per-order reply.
struct ReplayRequest {
  DatasetId dataset{};
  std::uint64_t input_seq = 0;
  std::uint64_t order_id = 0;
  std::uint64_t event_time_ns = 0;
  SymbolName symbol{};
  Price price = 0;
  Quantity quantity = 0;
  std::uint16_t shard = 0;
  ReplayOp operation = ReplayOp::Begin;
  Side side = Side::Buy;
  TimeInForce tif = TimeInForce::GTC;
  std::uint8_t flags = 0;
};
// Only Begin/Barrier return this marker, after both persistence writers sync.
struct ReplayReady {
  DatasetId dataset{};
  std::uint64_t input_seq = 0;
  std::uint64_t committed_seq = 0;
  std::uint64_t event_time_ns = 0;
  std::uint16_t shard = 0;
  std::uint16_t status = 0;  // 0 ready, 1 dataset mismatch, 2 barrier mismatch
  ReplayOp operation = ReplayOp::Begin;
};
#pragma pack(pop)
static_assert(sizeof(ReplayRequest) == 110);
static_assert(sizeof(ReplayReady) == 61);
}  // namespace marketlab::orders
