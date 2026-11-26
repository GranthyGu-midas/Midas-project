# Trading client and protocol v3

The engine supports decimal limit orders, GTC/IOC, full cancellation, same-price size reductions, and Coinbase Exchange's default decrement-and-cancel (dc) self-trade prevention. Public market data remains five levels per side, sampled every 200 ms.

## Console

Start `./src/scripts/run_client.sh --user alice`, or supply credentials to `build/bin/gateway_client` for automation. Commands:

```text
instrument BTC
buy BTC 60000.25 0.01234567 GTC
sell ETH 2500.50 0.25 IOC
reduce ORDER_ID 0.01
cancel ORDER_ID
cancel-last
relogin
trades
quit
```

Replace `ORDER_ID` with the ID returned by the engine. `GTC` is optional and is the default. `reduce` specifies the new **total target including cumulative fills**, not an amount to subtract or a new remaining size. Example: an order originally for 1 BTC, with 0.2 filled, reduced to 0.7, retains 0.5 on the book and records 0.3 canceled. The order ID and FIFO position are unchanged.

A target below cumulative fills, above the current effective total, off the quantity step, or referring to an ended/foreign order is rejected. Equality with cumulative fills cancels all remaining quantity; equality with the effective total is a no-op acknowledgement. These boundaries are explicit simulator rules. Processing uses the state at arrival at the matching thread. Requests whose outcome is unknown after disconnect are not automatically replayed; reconnect retrieves active orders, and `trades` retrieves actual fills.

`instrument SYMBOL` queries the server's increments. Production configuration currently sets BTC and ETH price tick to 0.01 and quantity step to 0.00000001. These are configurable simulation specifications; prices are not anchored to a reference price. The legacy second column of `symbol=NAME,REFERENCE,SHARD` is retained for config compatibility and has no pricing effect. Add `instrument=NAME,PRICE_TICK,QTY_STEP` to specify increments. Legacy fixtures without this line allow the minimum representable unit (0.00000001).

## Decimal representation

Every price and individual quantity in protocol v3 is a signed 64-bit integer with the universal scale **100000000**, not a product-dependent number of ticks. For example, 100.25 is encoded as 10025000000, and 0.01234567 as 1234567. Product ticks/steps are expressed in the same units and validated by the engine. Console and C++ helpers parse decimal strings exactly; binary floating-point conversion is not used. Up to eight fractional digits are supported; additional trailing zeroes are accepted, other excess precision and overflow are rejected. The maximum individual value is 92233720368.54775807. New orders require positive price and quantity. Aggregate market quantities use uint64 with the same scale; aggregate overflow rejects a new resting order before mutation.

C++ callers use `shared_data/orders/protocol.h` and `shared_data/orders/decimal.hpp` (`parse_decimal`, `format_decimal`). Python callers can use `Decimal`, multiplying by 100000000 and checking integrality and range before packing integers. The CLI does this conversion for users; raw wire clients must do it themselves.

## Public TCP wire format

The packed order protocol is little-endian. The 14-byte header is `uint16 messageType, uint32 messageLength, uint64 requestId`; length includes the header. Request IDs are nonzero correlation IDs, not persistent deduplication keys. Payloads are laid out exactly as in `protocol.h`. Fixed-width strings are 32 bytes, NUL terminated and zero padded. Default dc behavior is mandatory; there is no self-trading option.

| Message | Type | Total bytes | Payload after header |
| --- | ---: | ---: | --- |
| LoginRequest | 1 | 80 | username[32], password[32], uint16 version=3 |
| LoginAccepted | 2 | 24 | uint64 timestamp, uint16 version=3 |
| NewOrderRequest | 3 | 64 | symbol[32], uint8 side, int64 price, int64 quantity, uint8 timeInForce |
| CancelRequest | 4 | 22 | uint64 orderId |
| OrderUpdate | 5 | 106 | uint64 timestamp, OrderState, uint8 reason |
| Trade | 6 | 87 | uint64 timestamp, tradeId, orderId; symbol[32]; uint8 side; int64 price, quantity |
| ActiveOrderSnapshot | 7 | 106 | uint64 timestamp, uint8 isEnd, OrderState |
| RequestRejected | 8 | 24 | uint64 timestamp, uint16 reason |
| TradeHistoryRequest | 9 | 34 | Existing shard/limit/afterOffset/throughOffset query |
| TradeHistoryRecord | 10 | 158 | Original timestamp/tradeId, symbol, int64 price/quantity, order IDs and account names |
| TradeHistoryEnd | 11 | 46 | Existing history cursor and completion marker |
| ReduceRequest | 12 | 30 | uint64 orderId, int64 newTotalQuantity |
| InstrumentRequest | 13 | 46 | symbol[32] |
| InstrumentInfo | 14 | 70 | uint64 timestamp, symbol[32], int64 priceTick, quantityStep |

Side: Buy=1, Sell=2. TimeInForce: GTC=0, IOC=1. An IOC matches only at its limit or better, then expires its residual without resting. Fully executed IOC orders are Filled. STP can end an order as Canceled before IOC expiration.

`OrderState` is 83 bytes: `orderId:uint64, symbol:char[32], side:uint8, price:int64, originalQuantity:int64, filledQuantity:int64, remainingQuantity:int64, status:uint8, canceledQuantity:int64, timeInForce:uint8`. Python layout: `<Q32sBqqqqBqB`.

The invariant is `original = filled + remaining + canceled`. Original quantity is immutable. Canceled quantity includes user cancellations, reductions, IOC expiration and STP removals. Status values are Accepted=1, PartiallyFilled=2, Filled=3, Canceled=4, Expired=5. A reduction preserves the active Accepted/PartiallyFilled status unless it removes the entire remainder. If the reduced remainder subsequently fills, status is Filled even though cumulative fills are less than the original quantity.

Update reason values: Normal=0, Reduced=1, UserCancel=2, IOC=3, SelfTradePrevention=4. Rejection values: InvalidCredentials=1, InvalidOrder=2, CancelFailed=3, InvalidHistoryQuery=4, HistoryUnavailable=5, HistoryBusy=6, ReduceFailed=7, UnknownInstrument=8. A correlated acceptance precedes subsequent unsolicited execution/expiry/STP updates. Only actual trades have a trade ID and appear in trade history. dc cancels the smaller of two crossing same-account remainders and subtracts it from the larger, or cancels both if equal; it never creates a trade. Distinct simulated traders need distinct accounts.

## Market data and recovery compatibility

UDP and TCP snapshots have a versioned 36-byte header: `<BB16sHQQ` = version 3, kind (Delta=1/Snapshot=2), epoch, symbol ID, batch index, Unix-nanosecond timestamp. Each level is 25 bytes: `<BqQQ` = side, price, aggregate quantity, order count. Both price and quantity use scale 1e8. At most five levels per side appear in snapshots. `snapshot_every=10` still controls periodic full snapshots; intervening 200ms updates may be deltas. TCP queries return the last completed publication batch. Shard views and shared-memory transport are versioned as well.

Order protocol, internal links, persistence frames and snapshots have advanced to v3. Upgrade all components and clients together. Old files are not silently reinterpreted or deleted: v2 recovery is rejected. Use an isolated new data directory for v3; migrating old logs requires an explicit conversion. Tick/step configuration is part of the recovery fingerprint and must match the persisted deployment.

Partial reductions persist final order state and original arrival rank. Recovery and A/B replication apply these states without rematching or subtracting quantities again. Orders already canceled, expired or filled do not reappear. Notifications and committed market views retain the dual-log durability barrier. A crash before acknowledgement can still leave a committed operation; reconnect and reconcile instead of assuming failure.

Rules referenced: [Coinbase order modification and FIFO](https://docs.cdp.coinbase.com/exchange/fix-api/order-entry-messages/order-entry-messages5#ordercancelreplacerequest-35g), [IOC](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/orders/create-new-order#time-in-force), [dc self-trade prevention](https://docs.cdp.coinbase.com/exchange/concepts/matching-engine#self-trade-prevention). The simulator implements this subset, without Coinbase's PPP protections, balance checks, fees or full FIX session behavior.
