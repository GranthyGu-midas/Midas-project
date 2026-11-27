#include "matching_engine/core/engine.hpp"

#include <algorithm>
#include <cassert>
#include <limits>

namespace marketlab::orders {
namespace {
constexpr std::uint64_t sequence_limit = (1ULL << 48) - 1;

std::uint64_t allocate(std::uint16_t shard, std::uint64_t& sequence) {
  if (sequence > sequence_limit)
    throw std::overflow_error("Shard ID sequence exhausted");
  return (static_cast<std::uint64_t>(shard) << 48) | sequence++;
}
}  // namespace

MatchingEngine::MatchingEngine(const OrderConfig& config, std::uint16_t shard)
    : shard_(shard),
      epoch_(md::new_epoch()),
      users_(config.accounts.size() + 1, nullptr),
      user_order_counts_(config.accounts.size() + 1, 0) {
  if (!shard || config.products.size() > 65535)
    throw std::runtime_error("Invalid shard/products");
  const auto count =
      std::count_if(config.products.begin(), config.products.end(), [&](const auto& p) {
        return p.shard == shard;
      });
  if (!count)
    throw std::runtime_error("Shard has no products");
  books_.reserve(static_cast<std::size_t>(count));
  symbols_.reserve(static_cast<std::size_t>(count));
  for (std::size_t i = 0; i < config.products.size(); ++i) {
    const auto& product = config.products[i];
    if (product.shard != shard)
      continue;
    books_.emplace_back(&price_memory_);
    auto& book = books_.back();
    put_string(book.symbol, product.symbol);
    book.symbol_id = static_cast<std::uint16_t>(i + 1);
    if (product.price_tick <= 0 || product.quantity_step <= 0)
      throw std::runtime_error("Invalid instrument increments");
    book.price_tick = product.price_tick;
    book.quantity_step = product.quantity_step;
    symbols_.emplace(std::string_view(book.symbol, product.symbol.size()), books_.size() - 1);
  }
  usernames_.resize(config.accounts.size() + 1);
  for (std::size_t i = 0; i < config.accounts.size(); ++i)
    std::copy(
        config.accounts[i].name.begin(), config.accounts[i].name.end(), usernames_[i + 1].begin());
  outgoing_.reserve(64);
}

MatchingEngine::Order* MatchingEngine::Pool::acquire() {
  if (!free_) {
    auto block = std::make_unique<Order[]>(block_size);
    for (std::size_t i = 0; i + 1 < block_size; ++i)
      block[i].next = &block[i + 1];
    auto* first = block.get();
    blocks_.push_back(std::move(block));
    free_ = first;
  }
  auto* order = free_;
  free_ = free_->next;
  *order = Order{};
  return order;
}

void MatchingEngine::Pool::release(Order* order) noexcept {
  order->next = free_;
  free_ = order;
}

template <typename T>
void MatchingEngine::queue(LinkKind kind, const LinkHeader& context, const T& record) {
  static_assert(sizeof(T) <= sizeof(Event::payload));
  // Q is a one-way background producer. Keep its book/trade persistence, but
  // never construct per-order delivery events (including later maker fills).
  if constexpr (std::is_same_v<T, OrderUpdate>) {
    if (record.order.orderId & replay_id_bit)
      return;
  } else if constexpr (std::is_same_v<T, Trade>) {
    if (record.orderId & replay_id_bit)
      return;
  } else if constexpr (std::is_same_v<T, RequestRejected>) {
    if (context.kind == LinkKind::Replay) {
      ++replay_rejections_;
      return;
    }
  }
  Event event{};
  event.kind = kind;
  event.context = context;
  event.size = sizeof(T);
  std::memcpy(event.payload.data(), &record, sizeof(T));
  if (capture_)
    capture_->add(capture_->events, event);
  else
    outgoing_.push_back(event);
}

void MatchingEngine::command(const LinkHeader& h,
                             std::span<const std::uint8_t> bytes,
                             const Emit& emit) {
  // The reactor exclusively owns all live state; readers use copied snapshots.
  outgoing_.clear();
  if (!h.user || h.user >= users_.size())
    throw std::runtime_error("Unknown engine user");
  if (h.kind == LinkKind::Replay)
    replay(h, copy_record<ReplayRequest>(bytes));
  else if (h.kind == LinkKind::NewOrder)
    new_order(h, copy_record<NewOrderRequest>(bytes));
  else if (h.kind == LinkKind::Cancel)
    cancel(h, copy_record<CancelRequest>(bytes));
  else if (h.kind == LinkKind::Reduce)
    reduce(h, copy_record<ReduceRequest>(bytes));
  else if (h.kind == LinkKind::Snapshot)
    snapshot(h, copy_record<Header>(bytes));
  else if (h.kind == LinkKind::History)
    queue(LinkKind::Event,
          h,
          rejected(copy_record<TradeHistoryRequest>(bytes).header.requestId,
                   RejectReason::HistoryUnavailable));
  else
    throw std::runtime_error("Unexpected engine command");
  for (const auto& event : outgoing_)
    emit(event.kind, event.context, {event.payload.data(), event.size});
}

void MatchingEngine::execute(const LinkHeader& h,
                             std::span<const std::uint8_t> bytes,
                             persistence::Result& result) {
  if (capture_)
    throw std::logic_error("Reentrant matching command");
  capture_ = &result;
  try {
    command(h, bytes, [](auto, const auto&, auto) {
    });
    capture_ = nullptr;
  } catch (...) {
    capture_ = nullptr;
    throw;
  }
}

void MatchingEngine::change(const Order& o, bool erase) {
  if (!capture_)
    return;
  persistence::Change c;
  c.erase = erase;
  c.order = persistence::compact_order(o.state, o.user, o.book->symbol_id, o.arrival);
  capture_->add(capture_->changes, c);
}

void MatchingEngine::replay(const LinkHeader& h, const ReplayRequest& q) {
  if (q.shard != shard_ || empty_dataset(q.dataset))
    throw std::runtime_error("Invalid replay shard/dataset");
  if (q.operation == ReplayOp::Begin || q.operation == ReplayOp::Barrier) {
    ReplayReady ready{};
    ready.shard = shard_;
    ready.operation = q.operation;
    if (q.operation == ReplayOp::Begin && empty_dataset(replay_.dataset))
      replay_.dataset = q.dataset;
    if (q.dataset != replay_.dataset)
      ready.status = 1;
    else if (q.operation == ReplayOp::Barrier && q.input_seq != replay_.last_input)
      ready.status = 2;
    ready.dataset = replay_.dataset;
    ready.input_seq = replay_.last_input;
    ready.event_time_ns = replay_.last_event_ns;
    queue(LinkKind::ReplayReady, h, ready);
    return;
  }
  if (q.dataset != replay_.dataset || !q.input_seq || !q.event_time_ns ||
      !(q.order_id & replay_id_bit) || (q.order_id >> 48) != shard_ ||
      !(q.order_id & order_ordinal_mask) || (q.side != Side::Buy && q.side != Side::Sell))
    throw std::runtime_error("Invalid replay input identity");
  // Reconnects may overlap an already executed prefix. Dataset identity makes
  // input_seq stable; no second order-ID map or per-event receipt is required.
  if (q.input_seq <= replay_.last_input)
    return;
  const auto symbol = fixed_string(q.symbol);
  const auto found = symbols_.find(symbol);
  if (found == symbols_.end())
    throw std::runtime_error("Unknown replay product");
  replay_.last_input = q.input_seq;
  replay_.last_event_ns = q.event_time_ns;
  if (q.operation == ReplayOp::New) {
    const auto ordinal = q.order_id & order_ordinal_mask;
    if (ordinal < replay_.next_order)
      throw std::runtime_error("Reused replay order ID");
    replay_.next_order = ordinal + 1;  // Consume even a business-rule rejection.
    NewOrderRequest order{};
    order.header = {MessageType::NewOrderRequest, sizeof(order), q.input_seq};
    std::memcpy(order.symbol, q.symbol, sizeof(SymbolName));
    order.side = q.side;
    order.price = q.price;
    order.quantity = q.quantity;
    order.timeInForce = q.tif;
    new_order(h, order, q.order_id);
  } else if (q.operation == ReplayOp::Cancel) {
    CancelRequest request{{MessageType::CancelRequest, sizeof(CancelRequest), q.input_seq},
                          q.order_id};
    cancel(h, request);
  } else if (q.operation == ReplayOp::ReduceBy) {
    auto* current = active_.find(q.order_id);
    if (!current || current->user != h.user || q.quantity <= 0 ||
        current->book != &books_[found->second] || current->state.side != q.side ||
        q.quantity % current->book->quantity_step) {
      ++replay_rejections_;
      return;
    }
    ReduceRequest request{};
    request.header = {MessageType::ReduceRequest, sizeof(request), q.input_seq};
    request.orderId = q.order_id;
    const Quantity requested = q.quantity;
    const Quantity remaining = current->state.remainingQuantity;
    request.newTotalQuantity =
        current->state.filledQuantity + remaining - std::min(requested, remaining);
    reduce(h, request);
  } else {
    throw std::runtime_error("Unknown replay operation");
  }
}

void MatchingEngine::restore(std::span<const persistence::StoredOrder> orders,
                             std::uint64_t next_order, std::uint64_t next_trade,
                             const ReplayState& replay) {
  if (next_order_ != 1 || next_trade_ != 1)
    throw std::logic_error("Restore into nonempty core");
  std::uint64_t previous = 0;
  for (const auto& stored : orders) {
    const auto id = OrderId(stored.state.orderId);
    const auto symbol = std::string_view(stored.state.symbol);
    const auto found = symbols_.find(symbol);
    const auto user = std::find(usernames_.begin() + 1, usernames_.end(), stored.username);
    if (found == symbols_.end() || user == usernames_.end() || stored.arrival <= previous ||
        stored.arrival >= next_order || !valid_order_id(id, shard_, next_order, replay))
      throw std::runtime_error("Invalid recovered order identity/FIFO");
    previous = stored.arrival;
    Order o;
    o.state = stored.state;
    o.arrival = stored.arrival;
    o.user = static_cast<std::uint32_t>(user - usernames_.begin());
    o.book = &books_[found->second];
    if (!valid_active_order(o.state) || o.state.price % o.book->price_tick ||
        o.state.originalQuantity % o.book->quantity_step ||
        o.state.filledQuantity % o.book->quantity_step ||
        o.state.canceledQuantity % o.book->quantity_step)
      throw std::runtime_error("Invalid recovered order or instrument increment");
    rest(o);
  }
  next_order_ = next_order;
  next_trade_ = next_trade;
  replay_ = replay;
}

void MatchingEngine::replicate(std::span<const persistence::Change> changes,
                               const persistence::OrderDictionary& dictionary,
                               const persistence::Boundary& boundary) {
  for (const auto& change : changes) {
    auto* existing = active_.find(change.order.id);
    if (change.erase) {
      if (!existing)
        continue;  // An incoming order may have filled without ever resting.
      auto& levels =
          existing->state.side == Side::Buy ? existing->book->bids : existing->book->asks;
      const auto position = existing->position;
      position->second.quantity -= existing->state.remainingQuantity;
      unlink(*existing);
      if (!position->second.count)
        levels.erase(position);
      active_.erase(change.order.id);
      pool_.release(existing);
      continue;
    }
    const auto stored = dictionary.expand(change.order);
    if (existing) {
      if (existing->user != change.order.user || existing->state.price != stored.state.price ||
          existing->state.side != stored.state.side ||
          existing->book->symbol_id != change.order.symbol || existing->arrival != stored.arrival)
        throw std::runtime_error("Replication changed resting order identity");
      auto& quantity = existing->position->second.quantity;
      quantity -= existing->state.remainingQuantity;
      quantity += stored.state.remainingQuantity;
      existing->state = stored.state;  // Preserve the price-time queue position.
    } else {
      const auto found = symbols_.find(std::string_view(stored.state.symbol));
      if (found == symbols_.end() ||
          !valid_order_id(change.order.id, shard_, boundary.next_order, boundary.replay) ||
          !stored.arrival || stored.arrival >= boundary.next_order)
        throw std::runtime_error("Replication order belongs to another shard");
      Order order;
      order.state = stored.state;
      order.arrival = stored.arrival;
      order.user = change.order.user;
      order.book = &books_[found->second];
      rest(order);
    }
  }
  next_order_ = boundary.next_order;
  next_trade_ = boundary.next_trade;
  replay_ = boundary.replay;
}

void MatchingEngine::update(const Order& order,
                            RequestId request,
                            const LinkHeader& context,
                            Timestamp time, UpdateReason reason) {
  OrderUpdate record{};
  record.header = {MessageType::OrderUpdate, sizeof(record), request};
  record.timestamp = time ? time : unix_ns();
  record.order = order.state;
  record.reason = reason;
  auto destination = context;
  destination.user = order.user;
  queue(LinkKind::Event, destination, record);
}

void MatchingEngine::trade(
    const Order& order, TradeId id, Price price, Quantity quantity, Timestamp time) {
  Trade record{};
  record.header = {MessageType::Trade, sizeof(record), 0};
  record.timestamp = time;
  record.tradeId = id;
  record.orderId = order.state.orderId;
  std::memcpy(record.symbol, order.state.symbol, sizeof(SymbolName));
  record.side = order.state.side;
  record.price = price;
  record.quantity = quantity;
  queue(LinkKind::Event, {0, LinkKind::Event, 0, order.user, 0}, record);
}

void MatchingEngine::new_order(const LinkHeader& h, const NewOrderRequest& request,
                               OrderId supplied_id) {
  // Validate fixed-width strings without allocating a temporary std::string.
  const auto end = std::find(std::begin(request.symbol), std::end(request.symbol), '\0');
  if (end == std::end(request.symbol) || !std::all_of(end, std::end(request.symbol), [](char c) {
        return c == 0;
      }))
    throw std::runtime_error("Invalid symbol wire field");
  const auto found = symbols_.find(
      std::string_view(request.symbol, static_cast<std::size_t>(end - request.symbol)));
  if (found == symbols_.end() || (request.side != Side::Buy && request.side != Side::Sell) ||
      request.price <= 0 || request.quantity <= 0 ||
      request.price % books_[found->second].price_tick ||
      request.quantity % books_[found->second].quantity_step ||
      (request.timeInForce != TimeInForce::GTC && request.timeInForce != TimeInForce::IOC) ||
      next_order_ > order_ordinal_mask || next_trade_ > sequence_limit) {
    queue(LinkKind::Event, h, rejected(request.header.requestId, RejectReason::InvalidOrder));
    return;
  }
  auto& book = books_[found->second];
  const auto& same_side = request.side == Side::Buy ? book.bids : book.asks;
  const Price limit_price = request.price;
  const auto level = same_side.find(limit_price);
  if (request.timeInForce == TimeInForce::GTC && level != same_side.end() &&
      level->second.quantity > std::numeric_limits<std::uint64_t>::max() -
                               static_cast<std::uint64_t>(request.quantity)) {
    queue(LinkKind::Event, h, rejected(request.header.requestId, RejectReason::InvalidOrder));
    return;
  }
  Order incoming{};
  incoming.user = h.user;
  incoming.book = &book;
  incoming.arrival = next_order_;
  incoming.state.orderId =
      supplied_id ? supplied_id : ((std::uint64_t(shard_) << 48) | next_order_);
  ++next_order_;
  std::memcpy(incoming.state.symbol, book.symbol, sizeof(SymbolName));
  incoming.state.side = request.side;
  incoming.state.price = request.price;
  incoming.state.originalQuantity = incoming.state.remainingQuantity = request.quantity;
  incoming.state.status = OrderStatus::Accepted;
  incoming.state.timeInForce = request.timeInForce;
  update(incoming, request.header.requestId, h);
  match(incoming, request.side == Side::Buy ? book.asks : book.bids);
  if (incoming.state.remainingQuantity && request.timeInForce == TimeInForce::IOC) {
    incoming.state.canceledQuantity += incoming.state.remainingQuantity;
    incoming.state.remainingQuantity = 0;
    incoming.state.status = OrderStatus::Expired;
    update(incoming, 0, {0, LinkKind::Event, 0, incoming.user, 0}, 0, UpdateReason::IOC);
  }
  if (incoming.state.remainingQuantity)
    rest(incoming);
  change(incoming, !incoming.state.remainingQuantity);
}

void MatchingEngine::match(Order& incoming, Levels& opposite) {
  while (incoming.state.remainingQuantity && !opposite.empty()) {
    auto best = incoming.state.side == Side::Buy ? opposite.begin() : std::prev(opposite.end());
    const auto price = best->first;
    if ((incoming.state.side == Side::Buy && price > incoming.state.price) ||
        (incoming.state.side == Side::Sell && price < incoming.state.price))
      break;
    auto* resting = best->second.head;  // No order-ID lookup in the fill loop.
    const Quantity incoming_remaining = incoming.state.remainingQuantity;
    const Quantity resting_remaining = resting->state.remainingQuantity;
    const Quantity quantity = std::min(incoming_remaining, resting_remaining);
    // Coinbase default dc: remove equal size from both orders without a trade.
    if (incoming.user == resting->user) {
      const auto time = unix_ns();
      for (auto* order : {&incoming, resting}) {
        order->state.remainingQuantity -= quantity;
        order->state.canceledQuantity += quantity;
        order->state.status = order->state.remainingQuantity
            ? (order->state.filledQuantity ? OrderStatus::PartiallyFilled : OrderStatus::Accepted)
            : OrderStatus::Canceled;
        update(*order, 0, {0, LinkKind::Event, 0, order->user, 0}, time,
               UpdateReason::SelfTradePrevention);
      }
      change(*resting, !resting->state.remainingQuantity);
      best->second.quantity -= static_cast<std::uint64_t>(quantity);
      if (!resting->state.remainingQuantity) {
        unlink(*resting);
        active_.erase(resting->state.orderId);
        pool_.release(resting);
        if (!best->second.count) opposite.erase(best);
      }
      continue;
    }
    const auto id = allocate(shard_, next_trade_);
    const auto time = unix_ns();
    if (capture_) {
      const auto* buy = incoming.state.side == Side::Buy ? &incoming : resting;
      const auto* sell = incoming.state.side == Side::Sell ? &incoming : resting;
      persistence::TradeRecord t;
      t.id = id;
      t.timestamp = time;
      std::copy(
          std::begin(incoming.state.symbol), std::end(incoming.state.symbol), t.symbol.begin());
      t.price = price;
      t.quantity = quantity;
      t.buy_order = buy->state.orderId;
      t.sell_order = sell->state.orderId;
      t.buyer = usernames_[buy->user];
      t.seller = usernames_[sell->user];
      capture_->add(capture_->trades, t);
    }
    for (auto* order : {&incoming, resting}) {
      order->state.filledQuantity += quantity;
      order->state.remainingQuantity -= quantity;
      order->state.status =
          order->state.remainingQuantity ? OrderStatus::PartiallyFilled : OrderStatus::Filled;
      trade(*order, id, price, quantity, time);
      update(*order, 0, {0, LinkKind::Event, 0, order->user, 0}, time);
    }
    change(*resting, !resting->state.remainingQuantity);
    best->second.quantity -= static_cast<std::uint64_t>(quantity);
    if (!resting->state.remainingQuantity) {
      unlink(*resting);
      active_.erase(resting->state.orderId);
      pool_.release(resting);
      if (!best->second.count)
        opposite.erase(best);
    }
  }
}

void MatchingEngine::rest(const Order& incoming) {
  auto* order = pool_.acquire();
  *order = incoming;
  auto& levels = order->state.side == Side::Buy ? order->book->bids : order->book->asks;
  const Price price = order->state.price;  // Never bind STL references to packed fields.
  const auto [position, inserted] = levels.try_emplace(price);
  (void)inserted;
  auto& level = position->second;
  if (level.quantity > std::numeric_limits<std::uint64_t>::max() -
                       static_cast<std::uint64_t>(order->state.remainingQuantity)) {
    pool_.release(order);
    throw std::overflow_error("Price level aggregate overflow");
  }
  active_.insert(order->state.orderId, order);
  order->position = position;
  order->prev = level.tail;
  if (level.tail)
    level.tail->next = order;
  else
    level.head = order;
  level.tail = order;
  level.quantity += static_cast<std::uint64_t>(order->state.remainingQuantity);
  ++level.count;
  order->user_next = users_[order->user];
  if (order->user_next)
    order->user_next->user_prev = order;
  users_[order->user] = order;
  ++user_order_counts_[order->user];
}

void MatchingEngine::unlink(Order& order) {
  auto& level = order.position->second;
  if (order.prev)
    order.prev->next = order.next;
  else
    level.head = order.next;
  if (order.next)
    order.next->prev = order.prev;
  else
    level.tail = order.prev;
  --level.count;
  if (order.user_prev)
    order.user_prev->user_next = order.user_next;
  else
    users_[order.user] = order.user_next;
  if (order.user_next)
    order.user_next->user_prev = order.user_prev;
  --user_order_counts_[order.user];
}

void MatchingEngine::cancel(const LinkHeader& h, const CancelRequest& request, UpdateReason reason) {
  auto* order = active_.find(request.orderId);
  if (!order || order->user != h.user) {
    queue(LinkKind::Event, h, rejected(request.header.requestId, RejectReason::CancelFailed));
    return;
  }
  auto& levels = order->state.side == Side::Buy ? order->book->bids : order->book->asks;
  const auto position = order->position;
  position->second.quantity -= static_cast<std::uint64_t>(order->state.remainingQuantity);
  unlink(*order);
  if (!position->second.count)
    levels.erase(position);
  order->state.canceledQuantity += order->state.remainingQuantity;
  order->state.remainingQuantity = 0;
  order->state.status = OrderStatus::Canceled;
  update(*order, request.header.requestId, h, 0, reason);
  change(*order, true);
  active_.erase(request.orderId);
  pool_.release(order);
}

void MatchingEngine::reduce(const LinkHeader& h, const ReduceRequest& request) {
  auto* order = active_.find(request.orderId);
  if (!order || order->user != h.user || request.newTotalQuantity < 0 ||
      request.newTotalQuantity % order->book->quantity_step ||
      request.newTotalQuantity < order->state.filledQuantity ||
      request.newTotalQuantity > order->state.originalQuantity - order->state.canceledQuantity) {
    queue(LinkKind::Event, h, rejected(request.header.requestId, RejectReason::ReduceFailed));
    return;
  }
  if (request.newTotalQuantity == order->state.filledQuantity) {
    CancelRequest full{};
    full.header = request.header;
    full.orderId = request.orderId;
    cancel(h, full, UpdateReason::Reduced);
    return;
  }
  const auto remaining = request.newTotalQuantity - order->state.filledQuantity;
  const auto removed = order->state.remainingQuantity - remaining;
  order->position->second.quantity -= static_cast<std::uint64_t>(removed);
  order->state.remainingQuantity = remaining;
  order->state.canceledQuantity += removed;
  // In-place reduction retains the order ID and both FIFO links.
  if (removed) change(*order, false);
  update(*order, request.header.requestId, h, 0, UpdateReason::Reduced);
}

std::size_t MatchingEngine::active_order_count(std::uint32_t user) const {
  if (!user || user >= users_.size()) {
    throw std::runtime_error("Unknown snapshot user");
  }
  return user_order_counts_[user];
}

void MatchingEngine::copy_active_orders(std::uint32_t user,
                                        std::span<OrderState> destination) const {
  if (destination.size() != active_order_count(user)) {
    throw std::logic_error("Snapshot buffer does not match user order count");
  }
  std::size_t index = 0;
  for (const auto* order = users_[user]; order; order = order->user_next) {
    destination[index++] = order->state;
  }
  assert(index == destination.size());
}

void MatchingEngine::snapshot(const LinkHeader& h, const Header& request) {
  for (auto* order = users_[h.user]; order; order = order->user_next) {
    auto record =
        server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot, request.requestId);
    record.order = order->state;
    queue(LinkKind::Event, h, record);
  }
  auto end =
      server_record<ActiveOrderSnapshot>(MessageType::ActiveOrderSnapshot, request.requestId);
  end.isEnd = 1;
  queue(LinkKind::SnapshotEnd, h, end);
}

void MatchingEngine::copy_market(MarketSnapshot& view) noexcept {
  assert(view.books.size() == books_.size());
  view.epoch = epoch_;
  std::size_t index = 0;
  for (const auto& book : books_) {
    auto& top = view.books[index++];
    top.symbol_id = book.symbol_id;
    top.count = 0;
    auto copy = [&](auto begin, auto end, md::Side side) {
      for (unsigned count = 0; begin != end && count < md::depth; ++begin, ++count)
        top.levels[top.count++] = {side, begin->first, begin->second.quantity, begin->second.count};
    };
    copy(book.bids.rbegin(), book.bids.rend(), md::Side::Buy);
    copy(book.asks.begin(), book.asks.end(), md::Side::Sell);
  }
}
}  // namespace marketlab::orders
