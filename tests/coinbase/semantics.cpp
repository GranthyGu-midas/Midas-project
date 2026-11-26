#include "matching_engine/persistence/durable_engine.hpp"
#include "shared_data/orders/decimal.hpp"
#include <sys/wait.h>
#include <unistd.h>
#include <iostream>
#include <limits>
#include <random>

namespace m = marketlab;
namespace o = marketlab::orders;
namespace p = marketlab::orders::persistence;
namespace md = marketlab::md;
using namespace std::chrono_literals;
namespace {
void check(bool yes, const char* message) { if (!yes) throw std::runtime_error(message); }
template<class F> void rejects(F fn) {
  bool rejected = false; try { fn(); } catch (const std::exception&) { rejected = true; }
  check(rejected, "Expected rejection");
}
o::OrderConfig config() {
  o::OrderConfig c;
  c.accounts = {{"Alice","test"},{"Bob","test"},{"Carol","test"},{"Dave","test"}};
  c.shards = {{1,{"127.0.0.1",1}}};
  c.products = {{"BTC",100,1,m::parse_decimal("0.01"),1}};
  return c;
}
struct Response {
  struct Entry { unsigned user; o::Bytes bytes; };
  std::vector<Entry> entries;
  template<class T> std::vector<T> of(m::MessageType type, unsigned user = 0) const {
    std::vector<T> out;
    for (const auto& e : entries) {
      m::Header h; std::memcpy(&h,e.bytes.data(),sizeof(h));
      if (h.messageType == type && (!user || e.user == user)) out.push_back(o::copy_record<T>(e.bytes));
    }
    return out;
  }
  std::vector<m::Trade> trades(unsigned user = 0) const { return of<m::Trade>(m::MessageType::Trade,user); }
  std::vector<m::OrderUpdate> updates(unsigned user = 0) const { return of<m::OrderUpdate>(m::MessageType::OrderUpdate,user); }
  m::OrderId id() const { return updates().at(0).order.orderId; }
  bool rejected(m::RejectReason why) const {
    auto v = of<m::RequestRejected>(m::MessageType::RequestRejected);
    return v.size() == 1 && v[0].reason == why && entries.size() == 1;
  }
};
struct Client {
  o::EngineBackend& engine;
  std::uint64_t next = 1;
  template<class T> Response send(unsigned user, o::LinkKind kind, T request, m::MessageType type) {
    request.header = {type,sizeof(request),next++};
    Response result;
    engine.command({0,kind,request.header.requestId,user,1},o::record_bytes(request),
      [&](auto,const auto& h,auto b){result.entries.push_back({h.user,{b.begin(),b.end()}});});
    engine.drain();
    return result;
  }
  Response order(unsigned user,m::Side side,const char* qty,const char* price="100.25",m::TimeInForce tif=m::TimeInForce::GTC) {
    m::NewOrderRequest r{}; o::put_string(r.symbol,"BTC"); r.side=side;
    r.price=m::parse_decimal(price); r.quantity=m::parse_decimal(qty); r.timeInForce=tif;
    return send(user,o::LinkKind::NewOrder,r,m::MessageType::NewOrderRequest);
  }
  Response reduce(unsigned user,m::OrderId id,const char* qty) {
    m::ReduceRequest r{}; r.orderId=id; r.newTotalQuantity=m::parse_decimal(qty);
    return send(user,o::LinkKind::Reduce,r,m::MessageType::ReduceRequest);
  }
  Response cancel(unsigned user,m::OrderId id) {
    m::CancelRequest r{}; r.orderId=id;
    return send(user,o::LinkKind::Cancel,r,m::MessageType::CancelRequest);
  }
  std::vector<m::OrderState> active(unsigned user) {
    Response result;
    m::Header req{m::MessageType::ActiveOrderSnapshot,sizeof(req),next++};
    engine.command({0,o::LinkKind::Snapshot,req.requestId,user,1},o::record_bytes(req),
      [&](auto,const auto& h,auto b){result.entries.push_back({h.user,{b.begin(),b.end()}});});
    engine.drain();
    std::vector<m::OrderState> out;
    for (const auto& s:result.of<m::ActiveOrderSnapshot>(m::MessageType::ActiveOrderSnapshot))
      if (!s.isEnd) {check(m::valid_active_order(s.order),"Invalid active invariant");out.push_back(s.order);}
    return out;
  }
};
void numbers() {
  using m::parse_decimal; using m::format_decimal;
  for (const auto* s:{"0","0.00000001","100.25","0.01234567","92233720368.54775807"})
    check(format_decimal(parse_decimal(s))==s,"Decimal round trip");
  check(parse_decimal("1.2300000000")==123000000,"Exact trailing zero handling");
  for (const auto* s:{"",".1","1.","-1","+1","nan","1e3","1.000000001","92233720368.54775808","92233720369","99999999999999999999999"})
    rejects([&]{parse_decimal(s);});
  std::mt19937_64 random(402);
  for (unsigned i=0;i<10000;++i) {auto v=random()>>1;check(parse_decimal(format_decimal(v))==static_cast<std::int64_t>(v),"Random fixed-point round trip");}
  auto c=config(); o::MatchingEngine e(c,1); Client client{e};
  check(!client.order(1,m::Side::Buy,"0.01234567","60000.01").updates().empty(),"Old price band still enforced");
  check(client.order(1,m::Side::Buy,"1","100.001").rejected(m::RejectReason::InvalidOrder),"Off-tick price accepted");
  c.products[0].quantity_step=parse_decimal("0.001");
  o::MatchingEngine stepped(c,1); Client sc{stepped};
  check(sc.order(1,m::Side::Buy,"0.0001").rejected(m::RejectReason::InvalidOrder),"Off-step size accepted");
  auto id=sc.order(1,m::Side::Buy,"1").id();
  check(sc.reduce(1,id,"0.0001").rejected(m::RejectReason::ReduceFailed),"Off-step reduction accepted");
  const auto high=std::numeric_limits<std::int64_t>::max();
  md::Message msg{md::Kind::Snapshot,md::new_epoch(),1,0,1,{{md::Side::Buy,high,static_cast<std::uint64_t>(high),1}}};
  check(md::decode(md::encode(msg)).levels==msg.levels,"64-bit market round trip");
  auto old=md::encode(msg);old[0]=2;rejects([&]{md::decode(old);});
  o::MatchingEngine overflow(config(),1);Client oc{overflow};
  oc.order(1,m::Side::Buy,"92233720368.54775807");oc.order(2,m::Side::Buy,"92233720368.54775807");
  check(oc.order(3,m::Side::Buy,"0.00000002").rejected(m::RejectReason::InvalidOrder),"Level overflow accepted");
  check(overflow.market_view().books.at(1).levels.at(0).quantity==std::numeric_limits<std::uint64_t>::max()-1,"Overflow mutated book");
}
void reductions() {
  o::MatchingEngine engine(config(),1);Client c{engine};
  auto a=c.order(1,m::Side::Sell,"0.5").id();
  auto b=c.order(2,m::Side::Sell,"1").id();
  auto d=c.order(3,m::Side::Sell,"0.8").id();
  auto fill=c.order(4,m::Side::Buy,"0.7","100.25",m::TimeInForce::IOC);
  auto maker=fill.trades();check(maker.size()==4 && maker[1].orderId==a && maker[3].orderId==b,"Initial FIFO");
  check(c.reduce(1,b,"0.7").rejected(m::RejectReason::ReduceFailed),"Foreign reduction");
  check(c.reduce(2,b,"0.1").rejected(m::RejectReason::ReduceFailed),"Reduction below fills");
  check(c.reduce(2,b,"1.1").rejected(m::RejectReason::ReduceFailed),"Size increase accepted");
  auto reduced=c.reduce(2,b,"0.7").updates().at(0);
  check(reduced.reason==m::UpdateReason::Reduced && reduced.order.orderId==b &&
    reduced.order.originalQuantity==m::parse_decimal("1") && reduced.order.filledQuantity==m::parse_decimal("0.2") &&
    reduced.order.remainingQuantity==m::parse_decimal("0.5") && reduced.order.canceledQuantity==m::parse_decimal("0.3"),"Reduction quantities");
  check(c.reduce(2,b,"0.7").updates().at(0).order.canceledQuantity==m::parse_decimal("0.3"),"Repeated target reduced twice");
  auto book=engine.market_view().books.at(1);check(book.levels.size()==1 && book.levels[0].num_orders==2 && book.levels[0].quantity==static_cast<std::uint64_t>(m::parse_decimal("1.3")),"Reduced aggregate");
  auto next=c.order(4,m::Side::Buy,"0.6");maker=next.trades();
  check(maker.size()==4 && maker[1].orderId==b && maker[3].orderId==d,"Reduction lost queue priority");
  auto ended=next.updates(2).at(0).order;
  check(ended.status==m::OrderStatus::Filled && ended.filledQuantity==m::parse_decimal("0.7") && ended.canceledQuantity==m::parse_decimal("0.3"),"Filled after reduction");
  check(c.reduce(2,b,"0.7").rejected(m::RejectReason::ReduceFailed),"Ended reduction");
  auto canceled=c.reduce(3,d,"0.1").updates().at(0);
  check(canceled.order.status==m::OrderStatus::Canceled && canceled.order.canceledQuantity==m::parse_decimal("0.7"),"Target equal fills");
  check(engine.market_view().books.at(1).levels.empty(),"Reduction leaked book");
  auto z=c.order(1,m::Side::Buy,"1").id();
  check(c.reduce(1,z,"0").updates().at(0).order.status==m::OrderStatus::Canceled,"Zero target cancellation");
}
void ioc() {
  o::MatchingEngine engine(config(),1);Client c{engine};
  c.order(1,m::Side::Sell,"0.3","100.25");c.order(2,m::Side::Sell,"0.4","101");
  auto r=c.order(3,m::Side::Buy,"1","100.50",m::TimeInForce::IOC);
  auto end=r.updates(3).back();
  check(r.trades(3).size()==1 && end.order.status==m::OrderStatus::Expired && end.reason==m::UpdateReason::IOC &&
    end.order.filledQuantity==m::parse_decimal("0.3") && end.order.canceledQuantity==m::parse_decimal("0.7") && !end.order.remainingQuantity,"Partial IOC");
  check(c.active(3).empty() && engine.market_view().books.at(1).levels.at(0).price==m::parse_decimal("101"),"IOC rested/crossed limit");
  auto empty=c.order(3,m::Side::Buy,"1","100",m::TimeInForce::IOC);
  check(empty.trades().empty() && empty.updates().back().order.status==m::OrderStatus::Expired,"Zero-fill IOC rejected");
  auto full=c.order(3,m::Side::Buy,"0.4","101",m::TimeInForce::IOC);
  check(full.updates(3).back().order.status==m::OrderStatus::Filled,"Full IOC not filled");
  c.order(1,m::Side::Buy,"0.2","100");
  auto sell=c.order(3,m::Side::Sell,"0.5","100",m::TimeInForce::IOC);
  check(sell.trades(3).at(0).price==m::parse_decimal("100") && sell.updates(3).back().order.canceledQuantity==m::parse_decimal("0.3"),"Sell IOC");
}
void stp() {
  o::MatchingEngine engine(config(),1);Client c{engine};
  auto a=c.order(1,m::Side::Sell,"1").id();
  auto r=c.order(1,m::Side::Buy,"0.3");
  check(r.trades().empty() && engine.next_trade()==1,"STP generated trades");
  auto s=c.active(1).at(0);
  check(s.orderId==a && s.remainingQuantity==m::parse_decimal("0.7") && s.canceledQuantity==m::parse_decimal("0.3"),"STP maker decrement");
  c.order(4,m::Side::Buy,"0.2");
  auto b=c.order(2,m::Side::Sell,"0.2").id();
  r=c.order(1,m::Side::Buy,"0.8");
  check(r.trades(1).size()==1 && r.trades(2).at(0).orderId==b,"STP did not continue to another user");
  s=c.active(1).at(0);
  check(s.side==m::Side::Buy && s.filledQuantity==m::parse_decimal("0.2") && s.canceledQuantity==m::parse_decimal("0.5") && s.remainingQuantity==m::parse_decimal("0.1"),"STP incoming invariant");
  r=c.order(1,m::Side::Sell,"0.1");
  check(r.trades().empty() && c.active(1).empty(),"Equal STP orders not canceled");
  for(const auto& u:r.updates()) if(u.reason==m::UpdateReason::SelfTradePrevention)
    check(u.order.status==m::OrderStatus::Canceled,"Equal STP status");
}
p::Options options(const std::filesystem::path& path) {
  p::Options v;v.directory=path;v.checkpoint_interval=1h;return v;
}
void recovery(const std::filesystem::path& root) {
  auto conf=config();auto opt=options(root/"recovery");m::OrderId a=0,b=0;
  {
    p::DurableEngine e(conf,1,opt);Client c{e};
    a=c.order(1,m::Side::Sell,"1").id();b=c.order(2,m::Side::Sell,"1").id();
    c.order(4,m::Side::Buy,"0.2");e.request_checkpoint();e.wait_checkpoint();
    c.reduce(1,a,"0.7");c.reduce(1,a,"0.6");
    // No trades for the reduction-only batches; order deltas must still recover.
  }
  {
    p::DurableEngine e(conf,1,opt);Client c{e};auto s=c.active(1).at(0);
    check(s.orderId==a && s.remainingQuantity==m::parse_decimal("0.4") && s.canceledQuantity==m::parse_decimal("0.4"),"Snapshot + reduction delta recovery");
    auto r=c.order(4,m::Side::Buy,"0.5");auto trades=r.trades();
    check(trades.size()==4 && trades[1].orderId==a && trades[3].orderId==b,"Recovered reduction FIFO");
    c.reduce(2,b,"0.5");e.request_checkpoint();e.wait_checkpoint();
    c.order(2,m::Side::Buy,"0.1"); // STP only
    c.order(4,m::Side::Buy,"1","100.25",m::TimeInForce::IOC);
  }
  {
    p::DurableEngine e(conf,1,opt);Client c{e};
    for(unsigned u=1;u<=4;++u)check(c.active(u).empty(),"IOC/STP recovered as live orders");
    check(e.boundary().trade_count==4,"Recovery duplicated/missed trades");
  }
  auto altered=conf;altered.products[0].quantity_step=10;
  rejects([&]{p::DurableEngine e(altered,1,opt);});
}
void crash_reduction(const std::filesystem::path& root,p::Stage stage,bool committed,const std::string& tag) {
  auto opt=options(root/tag);auto conf=config();m::OrderId a=0;
  {p::DurableEngine e(conf,1,opt);Client c{e};a=c.order(1,m::Side::Sell,"1").id();c.order(2,m::Side::Sell,"1");e.request_checkpoint();e.wait_checkpoint();}
  const auto pid=fork();check(pid>=0,"fork failed");
  if(!pid) {
    opt.hook=[&](p::Stage at,std::uint64_t seq){if(seq==3 && at==stage)_exit(73);};
    p::DurableEngine e(conf,1,opt);Client c{e};c.reduce(1,a,"0.4");_exit(74);
  }
  int status=0;waitpid(pid,&status,0);check(WIFEXITED(status)&&WEXITSTATUS(status)==73,"Fault hook not reached");
  p::DurableEngine e(conf,1,opt);Client c{e};auto s=c.active(1).at(0);
  check(s.remainingQuantity==m::parse_decimal(committed?"0.4":"1"),"Crash reduction commit boundary");
  check(s.canceledQuantity==m::parse_decimal(committed?"0.6":"0"),"Crash canceled quantity");
  auto r=c.order(4,m::Side::Buy,"0.1");check(r.trades(1).at(0).orderId==a,"Crash changed FIFO");
}
void replica(const std::filesystem::path& root) {
  auto conf=config();o::MatchingEngine source(conf,1);p::OrderDictionary dictionary(conf);
  auto opt=options(root/"replica");std::uint64_t seq=0;m::OrderId a=0,b=0;
  {
    p::DurableEngine target(conf,1,opt);
    auto submit=[&](unsigned user,o::LinkKind kind,const auto& request){
      p::Result result;source.execute({0,kind,seq+1,user,1},o::record_bytes(request),result);
      p::Boundary bound{++seq,source.next_order(),source.next_trade()};o::Bytes ob,tb;
      p::start_frame(ob,p::Kind::Orders,bound,result.trades.size(),result.changes.size());
      for(const auto& change:result.changes){p::integer(ob,change.erase?0:1,1);if(change.erase)p::integer(ob,change.order.id,8);else p::encode_order(ob,dictionary.expand(change.order));}
      p::finish_frame(ob);std::optional<p::Frame> tf;
      if(!result.trades.empty()){p::start_frame(tb,p::Kind::Trades,bound,result.trades.size(),result.trades.size());for(const auto& t:result.trades)p::encode_trade(tb,t);p::finish_frame(tb);tf=p::decode_frame(tb);}
      target.replicate(p::decode_frame(ob),tf);target.drain();
      check(source.market_view().books==target.market_view().books,"Replica book mismatch");
      Client left{source},right{target};
      for(unsigned u=1;u<=4;++u){auto x=left.active(u),y=right.active(u);check(x.size()==y.size(),"Replica active count");for(std::size_t i=0;i<x.size();++i)check(std::memcmp(&x[i],&y[i],sizeof(m::OrderState))==0,"Replica order fields");}
      return result;
    };
    auto order=[&](unsigned user,m::Side side,const char* qty,m::TimeInForce tif=m::TimeInForce::GTC){m::NewOrderRequest r{};r.header={m::MessageType::NewOrderRequest,sizeof(r),seq+1};o::put_string(r.symbol,"BTC");r.side=side;r.price=m::parse_decimal("100.25");r.quantity=m::parse_decimal(qty);r.timeInForce=tif;return submit(user,o::LinkKind::NewOrder,r);};
    a=order(1,m::Side::Sell,"1").changes.back().order.id;b=order(2,m::Side::Sell,"1").changes.back().order.id;
    m::ReduceRequest reduce{{m::MessageType::ReduceRequest,sizeof(reduce),seq+1},a,m::parse_decimal("0.7")};submit(1,o::LinkKind::Reduce,reduce);
    order(1,m::Side::Buy,"0.1"); // STP decrements maker without changing its place.
    order(4,m::Side::Buy,"0.2",m::TimeInForce::IOC);
    target.request_checkpoint();target.wait_checkpoint();
  }
  p::DurableEngine restored(conf,1,opt);Client c{restored};
  auto r=c.order(4,m::Side::Buy,"0.5");auto trades=r.trades();
  check(trades.size()==4 && trades[1].orderId==a && trades[3].orderId==b,"Promotion/restored replica FIFO");
}
}
int main(int argc,char** argv){
  try{
    check(argc==2,"Expected result directory");auto root=std::filesystem::path(argv[1])/std::to_string(getpid());std::filesystem::create_directories(root);
    numbers();std::cout<<"PASS fixed-point, precision, overflow, wire version\n";
    reductions();std::cout<<"PASS reduce boundaries, races, FIFO, conservation\n";
    ioc();std::cout<<"PASS IOC zero/partial/full, buy/sell, price limit\n";
    stp();std::cout<<"PASS dc smaller/equal/larger, continue matching\n";
    recovery(root);std::cout<<"PASS snapshots + deltas, IOC/STP history, spec mismatch\n";
    crash_reduction(root,p::Stage::BeforeOrderWrite,false,"before-write");
    crash_reduction(root,p::Stage::OrderSynced,true,"after-sync");
    crash_reduction(root,p::Stage::Emitting,true,"before-ack");
    std::cout<<"PASS crash before-write/after-sync/before-ack\n";
    replica(root);std::cout<<"PASS A/B replication and durable promotion\n";return 0;
  }catch(const std::exception& e){std::cerr<<"FAIL "<<e.what()<<'\n';return 1;}
}
