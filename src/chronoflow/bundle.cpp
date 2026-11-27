#include "chronoflow/bundle.hpp"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <fstream>
#include <limits>
#include <set>

#include "chronoflow/json.hpp"
#include "chronoflow/sha256.hpp"

namespace midas::chronoflow {
namespace {
using namespace marketlab;
using namespace marketlab::orders;

void require(bool condition, const char* message) {
  if (!condition)
    throw std::runtime_error(message);
}

void require(bool condition, const std::string& message) {
  if (!condition)
    throw std::runtime_error(message);
}

DatasetId dataset_id(const std::string& text) {
  require(text.size() == 64, "Dataset ID must be a SHA-256 digest");
  DatasetId id{};
  for (std::size_t i = 0; i < id.size(); ++i) {
    unsigned value = 0;
    const auto begin = text.data() + i * 2;
    const auto result = std::from_chars(begin, begin + 2, value, 16);
    require(result.ec == std::errc{} && result.ptr == begin + 2, "Invalid dataset digest");
    id[i] = static_cast<std::uint8_t>(value);
  }
  require(!empty_dataset(id), "Empty dataset digest");
  return id;
}

std::uint16_t small_id(const Json& value) {
  const auto id = value.number();
  require(id > 0 && id <= 65535, "Manifest ID outside 1..65535");
  return static_cast<std::uint16_t>(id);
}

struct Actor {
  std::uint16_t symbol = 0;
  Side side{};
};
}  // namespace

RecordFile::RecordFile(const std::filesystem::path& path) {
  try {
    fd_ = open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd_ < 0)
      throw std::runtime_error("Cannot open replay file: " + path.string());
    struct stat info{};
    require(fstat(fd_, &info) == 0 && S_ISREG(info.st_mode) && info.st_size >= 64,
            "Invalid replay file length/type");
    size_ = static_cast<std::size_t>(info.st_size);
    auto* memory = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
    require(memory != MAP_FAILED, "Cannot map replay file");
    mapping_ = static_cast<const std::uint8_t*>(memory);
    std::memcpy(&header, mapping_, sizeof(header));
    require(std::memcmp(header.magic, "MIDASQ01", 8) == 0 && header.version == 1 &&
                header.header_bytes == 64 && header.record_bytes == 64,
            "Unsupported replay binary format");
    require(header.count <= (std::numeric_limits<std::size_t>::max() - 64) / 64 &&
                size_ == 64 + header.count * 64,
            "Truncated or trailing replay records");
    madvise(const_cast<std::uint8_t*>(mapping_), size_, MADV_WILLNEED);
  } catch (...) {
    if (mapping_)
      munmap(const_cast<std::uint8_t*>(mapping_), size_);
    if (fd_ >= 0)
      close(fd_);
    throw;
  }
}

RecordFile::~RecordFile() {
  if (mapping_)
    munmap(const_cast<std::uint8_t*>(mapping_), size_);
  if (fd_ >= 0)
    close(fd_);
}

Record RecordFile::at(std::uint64_t index) const {
  require(index < header.count, "Replay record index outside file");
  Record record{};
  std::memcpy(&record, mapping_ + 64 + index * 64, sizeof(record));
  return record;
}

std::span<const std::uint8_t> RecordFile::bytes() const {
  return {mapping_, size_};
}

Bundle::Bundle(const std::filesystem::path& directory, const OrderConfig& config)
    : actors(65536, 0), symbols(65536), bootstrap_last(65536, 0), final_last(65536, 0) {
  std::ifstream input(directory / "manifest.json");
  require(bool(input), "Cannot open replay manifest");
  const std::string contents{std::istreambuf_iterator<char>(input),
                             std::istreambuf_iterator<char>()};
  require(contents.size() < 16 * 1024 * 1024, "Replay manifest exceeds 16 MiB");
  const auto manifest = JsonReader(contents).parse();
  require(manifest.at("format").text() == "midas-normalized-order-flow" &&
              manifest.at("version").number() == 1 &&
              manifest.at("numeric_scale").number() == 100000000ULL,
          "Unsupported replay manifest format/scale");
  id = manifest.at("dataset_id").text();
  dataset = dataset_id(id);
  start_ns = manifest.at("start_ns").number();
  duration_ns = manifest.at("duration_ns").number();
  require(start_ns && duration_ns && duration_ns <= 7ULL * 86400 * 1000000000 &&
              start_ns <= std::numeric_limits<std::uint64_t>::max() - duration_ns &&
              manifest.at("end_ns").number() == start_ns + duration_ns,
          "Invalid replay time window");

  std::vector<std::uint16_t> product_shards(65536, 0);
  std::vector<std::int64_t> ticks(65536, 0);
  std::vector<std::int64_t> steps(65536, 0);
  std::set<std::uint16_t> unique_shards;
  for (const auto& product : manifest.at("products").array) {
    const auto symbol = small_id(product.at("symbol_id"));
    const auto shard = small_id(product.at("shard_id"));
    const auto name = product.at("symbol").text();
    require(symbol <= config.products.size() && symbols[symbol].empty(),
            "Invalid/duplicate product ID");
    const auto& configured = config.products[symbol - 1];
    require(configured.symbol == name && configured.shard == shard &&
                product.at("price_tick_e8").number() ==
                    static_cast<std::uint64_t>(configured.price_tick) &&
                product.at("quantity_step_e8").number() ==
                    static_cast<std::uint64_t>(configured.quantity_step),
            "Bundle product/increment differs from engine configuration: " + name);
    symbols[symbol] = name;
    product_shards[symbol] = shard;
    ticks[symbol] = configured.price_tick;
    steps[symbol] = configured.quantity_step;
    unique_shards.insert(shard);
  }
  shards.assign(unique_shards.begin(), unique_shards.end());
  require(!shards.empty(), "Replay bundle contains no products");
  std::vector<Actor> actor_info(65536);
  std::set<std::uint32_t> bound_users;
  for (const auto& actor : manifest.at("actors").array) {
    const auto logical = small_id(actor.at("actor_id"));
    const auto name = actor.at("name").text();
    const auto found =
        std::find_if(config.accounts.begin(), config.accounts.end(), [&](const auto& user) {
          return user.name == name;
        });
    require(found != config.accounts.end() && !actors[logical],
            "Missing/duplicate replay account: " + name);
    const auto user = static_cast<std::uint32_t>(found - config.accounts.begin() + 1);
    require(bound_users.insert(user).second, "Each logical replay actor needs a distinct account");
    const auto symbol = actor.at("symbol").text();
    const auto product = std::find(symbols.begin(), symbols.end(), symbol);
    require(product != symbols.end(), "Replay actor references unknown product");
    const auto side = actor.at("side").number();
    require(side == 1 || side == 2, "Invalid replay actor side");
    actors[logical] = user;
    actor_info[logical] = {static_cast<std::uint16_t>(product - symbols.begin()),
                           static_cast<Side>(side)};
  }
  for (const auto& name : {"bootstrap.bin", "events.bin"}) {
    auto file = std::make_unique<RecordFile>(directory / name);
    const auto& declared = manifest.at("files").at(name);
    require(file->bytes().size() == declared.at("bytes").number() &&
                sha256(file->bytes()) == declared.at("sha256").text(),
            "Replay binary checksum mismatch: " + std::string(name));
    require(file->header.dataset == dataset && file->header.start_ns == start_ns,
            "Replay file belongs to a different dataset/window");
    if (std::string_view(name) == "bootstrap.bin")
      bootstrap = std::move(file);
    else
      events = std::move(file);
  }
  require(bootstrap->header.count == manifest.at("counts").at("bootstrap").number() &&
              events->header.count == manifest.at("counts").at("replay").number(),
          "Manifest record counts disagree with binary files");

  // Startup-only validation. This is not a source-ID translation table on the
  // sending path: the temporary owner vectors are discarded after validation.
  std::map<std::uint16_t, std::vector<std::uint16_t>> owners;
  for (auto shard : shards)
    owners[shard].push_back(0);
  std::uint64_t expected = 1;
  std::uint64_t previous_offset = 0;
  for (const auto* file : {bootstrap.get(), events.get()}) {
    const bool initial = file == bootstrap.get();
    for (std::uint64_t i = 0; i < file->header.count; ++i) {
      const auto row = file->at(i);
      require(row.input_seq == expected++, "Noncontiguous replay input sequence");
      require(row.shard && row.symbol && product_shards[row.symbol] == row.shard && row.actor &&
                  actors[row.actor] && actor_info[row.actor].symbol == row.symbol &&
                  actor_info[row.actor].side == row.side,
              "Replay routing/ownership mismatch");
      require(row.event_time_ns >= start_ns && row.event_time_ns < start_ns + duration_ns &&
                  !(row.flags & ~31) &&
                  std::all_of(
                      std::begin(row.reserved), std::end(row.reserved),
                      [](auto b) {
                        return b == 0;
                      }),
              "Invalid replay timestamp/flags/reserved bytes");
      if (initial) {
        require(row.operation == ReplayOp::New && row.tif == TimeInForce::GTC && (row.flags & 1) &&
                    row.offset_ns == 0 && row.event_time_ns == start_ns,
                "Invalid bootstrap record");
        bootstrap_last[row.shard] = row.input_seq;
      } else {
        require(!(row.flags & 1) && row.offset_ns >= previous_offset &&
                    row.offset_ns < duration_ns && row.offset_ns >= row.event_time_ns - start_ns &&
                    (row.offset_ns == row.event_time_ns - start_ns || (row.flags & 16)),
                "Invalid replay time ordering");
        previous_offset = row.offset_ns;
      }
      const auto ordinal = row.order_id & order_ordinal_mask;
      require((row.order_id >> 48) == row.shard && (row.order_id & replay_id_bit) && ordinal,
              "Invalid normalized replay order ID");
      const std::uint16_t shard = row.shard;
      auto& ownership = owners.at(shard);
      if (row.operation == ReplayOp::New) {
        require(ordinal == ownership.size() && row.price > 0 && row.quantity > 0 &&
                    row.price % ticks[row.symbol] == 0 && row.quantity % steps[row.symbol] == 0 &&
                    (row.tif == TimeInForce::GTC || row.tif == TimeInForce::IOC),
                "Invalid replay new order");
        const std::uint16_t actor = row.actor;
        ownership.push_back(actor);
      } else {
        require(ordinal < ownership.size() && ownership[ordinal] == row.actor && row.price == 0 &&
                    row.tif == TimeInForce::GTC &&
                    ((row.operation == ReplayOp::Cancel && row.quantity == 0) ||
                     (row.operation == ReplayOp::ReduceBy && row.quantity > 0 &&
                      row.quantity % steps[row.symbol] == 0)),
                "Invalid replay cancel/reduction reference");
      }
      final_last[row.shard] = row.input_seq;
    }
  }
  for (auto shard : shards)
    require(bootstrap_last[shard] != 0, "Each replay shard needs a bootstrap book");
}

marketlab::orders::ReplayRequest Bundle::request(const Record& row) const {
  marketlab::orders::ReplayRequest request{};
  request.dataset = dataset;
  request.input_seq = row.input_seq;
  request.order_id = row.order_id;
  request.event_time_ns = row.event_time_ns;
  const auto& symbol = symbols[row.symbol];
  std::memcpy(request.symbol, symbol.data(), symbol.size());
  request.price = row.price;
  request.quantity = row.quantity;
  request.shard = row.shard;
  request.operation = row.operation;
  request.side = row.side;
  request.tif = row.tif;
  request.flags = row.flags;
  return request;
}

Record Bundle::sequence(std::uint64_t input_seq) const {
  require(input_seq > 0 && input_seq <= bootstrap->header.count + events->header.count,
          "Engine recovery point is outside this dataset");
  if (input_seq <= bootstrap->header.count)
    return bootstrap->at(input_seq - 1);
  return events->at(input_seq - bootstrap->header.count - 1);
}

std::uint32_t Bundle::control_user(std::uint16_t shard) const {
  for (std::uint64_t i = 0; i < bootstrap->header.count; ++i) {
    const auto row = bootstrap->at(i);
    if (row.shard == shard)
      return actors[row.actor];
  }
  throw std::runtime_error("No control account for replay shard");
}
}  // namespace midas::chronoflow
