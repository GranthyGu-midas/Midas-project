#pragma once
#include "shared_data/orders/config.hpp"
#include "shared_data/orders/replay.hpp"
#include <filesystem>
#include <memory>
#include <span>
#include <vector>

namespace midas::chronoflow {
#pragma pack(push, 1)
struct Record {
  std::uint64_t input_seq;
  std::uint64_t event_time_ns;
  std::uint64_t offset_ns;
  std::uint64_t order_id;
  std::int64_t price;
  std::int64_t quantity;
  std::uint16_t shard;
  std::uint16_t symbol;
  std::uint16_t actor;
  marketlab::orders::ReplayOp operation;
  marketlab::Side side;
  marketlab::TimeInForce tif;
  std::uint8_t flags;
  std::uint8_t reserved[6];
};
struct FileHeader {
  char magic[8];
  std::uint16_t version;
  std::uint16_t header_bytes;
  std::uint32_t record_bytes;
  std::uint64_t count;
  std::uint64_t start_ns;
  marketlab::orders::DatasetId dataset;
};
#pragma pack(pop)
static_assert(sizeof(Record) == 64 && sizeof(FileHeader) == 64);

class RecordFile {
 public:
  explicit RecordFile(const std::filesystem::path&);
  ~RecordFile();
  RecordFile(const RecordFile&) = delete;
  RecordFile& operator=(const RecordFile&) = delete;
  FileHeader header{};
  Record at(std::uint64_t index) const;
  std::span<const std::uint8_t> bytes() const;

 private:
  int fd_ = -1;
  const std::uint8_t* mapping_ = nullptr;
  std::size_t size_ = 0;
};

class Bundle {
 public:
  Bundle(const std::filesystem::path&, const marketlab::orders::OrderConfig&);
  std::string id;
  marketlab::orders::DatasetId dataset{};
  std::uint64_t start_ns = 0;
  std::uint64_t duration_ns = 0;
  std::unique_ptr<RecordFile> bootstrap;
  std::unique_ptr<RecordFile> events;
  std::vector<std::uint16_t> shards;
  std::vector<std::uint32_t> actors;
  std::vector<std::string> symbols;
  std::vector<std::uint64_t> bootstrap_last;
  std::vector<std::uint64_t> final_last;
  marketlab::orders::ReplayRequest request(const Record&) const;
  Record sequence(std::uint64_t input_seq) const;
  std::uint32_t control_user(std::uint16_t shard) const;
};
}  // namespace midas::chronoflow
