#!/usr/bin/env python3
"""Independent v3 TCP client, two shards, public feed, CLI, promotion and SIGKILL."""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import signal
import socket
import statistics
import struct
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
HEADER = struct.Struct('<HIQ')
STATE = struct.Struct('<Q32sBqqqqBqB')
LEVEL = struct.Struct('<BqQQ')
MD_HEADER = struct.Struct('<BB16sHQQ')
SCALE = 100000000

def check(value, message):
    if not value: raise AssertionError(message)

def fixed(text): return text.encode().ljust(32, b'\0')
def units(text):
    result = Decimal(str(text)) * SCALE
    check(result == result.to_integral_value(), 'test input precision')
    return int(result)

def exact(sock, length):
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length-len(data))
        if not chunk: raise EOFError('peer closed')
        data.extend(chunk)
    return bytes(data)

def eventually(fn, message, timeout=12):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            value=fn()
            if value: return value
        except (OSError, EOFError): pass
        time.sleep(.03)
    raise AssertionError(message)

class Client:
    def __init__(self, port, user):
        self.sock=socket.create_connection(('127.0.0.1',port),timeout=10)
        self.user=user; self.sequence=0; self.events=[]; self.snapshot()
    def close(self): self.sock.close()
    def send(self, kind, payload):
        self.sequence+=1
        self.sock.sendall(HEADER.pack(kind,HEADER.size+len(payload),self.sequence)+payload)
        return self.sequence
    def receive(self):
        kind,length,req=HEADER.unpack(exact(self.sock,HEADER.size))
        check(HEADER.size<=length<=158,'server frame size')
        data=exact(self.sock,length-HEADER.size)
        self.events.append((kind,req,data))
        return kind,req,data
    def request(self, kind, payload):
        req=self.send(kind,payload)
        while True:
            k,r,data=self.receive()
            if r==req: return k,data
    def order(self, symbol, side, price, qty, tif=0):
        k,data=self.request(3,struct.pack('<32sBqqB',fixed(symbol),side,units(price),units(qty),tif))
        check(k==5,'order rejected')
        return STATE.unpack_from(data,8)[0]
    def reduce(self, oid, total):
        k,data=self.request(12,struct.pack('<Qq',oid,units(total)))
        check(k==5 and data[-1]==1,'reduction acknowledgement')
        return STATE.unpack_from(data,8)
    def snapshot(self):
        req=self.send(1,fixed(self.user)+fixed('test')+struct.pack('<H',3))
        orders={}; accepted=False
        while True:
            k,r,data=self.receive()
            if r!=req: continue
            if k==2:
                check(struct.unpack_from('<H',data,8)[0]==3,'login protocol version');accepted=True
            else:
                check(k==7 and accepted,'snapshot framing')
                if data[8]: return orders
                order=STATE.unpack_from(data,9);orders[order[0]]=order
    def history(self):
        req=self.send(9,struct.pack('<HHQQ',0,256,0,0)); trades=[]
        while True:
            k,r,data=self.receive()
            if r!=req: continue
            if k==10: trades.append(struct.unpack('<QQ32sqqQQ32s32s',data))
            else:
                check(k==11,'history response')
                _,shard,flags,count,cursor,bound=struct.unpack('<QHHIQQ',data)
                check(not(flags&1),'test unexpectedly needs another page')
                if not shard: return trades

def rpc(port, command):
    with socket.create_connection(('127.0.0.1',port),timeout=60) as conn:
        conn.sendall(command.encode()+b'\n')
        with conn.makefile('rb') as stream: return json.loads(stream.readline())

def market(port, symbol):
    with socket.create_connection(('127.0.0.1',port),timeout=3) as conn:
        conn.sendall(struct.pack('<H',symbol));length,=struct.unpack('<H',exact(conn,2));data=exact(conn,length)
    header=MD_HEADER.unpack_from(data)
    check(header[0:2]==(3,2) and header[3]==symbol,'market snapshot v3')
    return header,[LEVEL.unpack_from(data,i) for i in range(MD_HEADER.size,len(data),LEVEL.size)]

class Deployment:
    def __init__(self, binaries, root):
        self.bin=binaries;self.root=root;self.children=[];self.logs=[]
        reserved=[socket.socket() for _ in range(19)]
        for s in reserved:s.bind(('127.0.0.1',0))
        ports=iter(s.getsockname()[1] for s in reserved)
        self.gateway,self.router,self.admin,self.control,self.query=[next(ports) for _ in range(5)]
        self.udp=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);self.udp.bind(('127.0.0.1',0));self.udp.settimeout(5)
        self.topology={'router_admin_port':self.admin,'supervisor_port':self.control,'shards':{}}
        conf=['routing=router',f'gateway_listen=127.0.0.1,{self.gateway}',f'router_listen=127.0.0.1,{self.router}',f'engine_endpoint=127.0.0.1,{self.router}']
        conf += [f'user={u},test' for u in ('Alice','Bob','Carol','Dave')]
        pub=['symbols=1:BTC,2:ETH',f'udp_receiver=127.0.0.1,{self.udp.getsockname()[1]}','query_host=127.0.0.1',f'query_port={self.query}']
        for shard,symbol in ((1,'BTC'),(2,'ETH')):
            cfg={'market':next(ports)}
            for slot in ('A','B'):cfg[slot]=dict(zip(('order','control','market'),[next(ports) for _ in range(3)]))
            self.topology['shards'][str(shard)]=cfg
            conf += [f'shard={shard},127.0.0.1,{cfg["A"]["order"]}',f'symbol={symbol},100,{shard}',f'instrument={symbol},0.01,0.00000001']
            pub += [f'shard=s{shard},tcp,127.0.0.1,{cfg["market"]},{root}/unused-{shard}.mmap,{shard}']
        self.config=root/'exchange.conf';self.config.write_text('\n'.join(conf)+'\n')
        self.pub=root/'publisher.conf';self.pub.write_text('\n'.join(pub)+'\n')
        cluster=root/'cluster.json';cluster.write_text(json.dumps(self.topology))
        for s in reserved:s.close()
        self.args=[sys.executable,'-B',ROOT/'src/matching_engine/cluster/run.py','--config',self.config,'--market-config',self.pub,'--cluster-config',cluster,'--build-dir',binaries,'--data-dir',root/'data','--log-dir',root/'cluster-logs','--copy-mib-per-second','0']
        self.cluster=self.launch(self.args,'cluster.log')
        self.launch([binaries/'market_gateway','gateway','--config',self.config],'gateway.log')
        self.launch([binaries/'market_broadcast','publish','--config',self.pub],'publisher.log')
        eventually(self.ready,'deployment not ready')
    def launch(self,args,name,stdin=subprocess.DEVNULL):
        stream=(self.root/name).open('w');self.logs.append(stream)
        proc=subprocess.Popen(list(map(str,args)),stdin=stdin,stdout=stream,stderr=subprocess.STDOUT)
        self.children.append(proc);return proc
    def ready(self):
        if not rpc(self.control,'status')['ok']:return False
        c=Client(self.gateway,'Dave');c.close();return True
    def close(self):
        self.udp.close()
        for p in reversed(self.children):
            if p.poll() is None:p.terminate()
            try:p.wait(timeout=15)
            except subprocess.TimeoutExpired:p.kill();p.wait()
        for f in self.logs:f.close()

def exercise(binary,root):
    ex=Deployment(binary,root);clients=[]
    try:
        alice,bob,carol=[Client(ex.gateway,u) for u in ('Alice','Bob','Carol')];clients += [alice,bob,carol]
        k,spec=alice.request(13,fixed('BTC'));check(k==14 and struct.unpack_from('<qq',spec,40)==(units('.01'),1),'instrument API')
        # Real decimal orders far outside the old 90..110 band.
        a=alice.order('BTC',2,'60000.25','1');b=bob.order('BTC',2,'60000.25','1')
        carol.order('BTC',1,'60000.25','.2',1);carol.snapshot();alice.snapshot()
        reduced=alice.reduce(a,'.7');check(reduced[5:7]==(units('.2'),units('.5')) and reduced[8]==units('.3'),'network reduction')
        eth=bob.order('ETH',1,'2000.50','.12345678');check(eth>>48==2 and a>>48==1,'two-shard routing')
        eventually(lambda: market(ex.query,1)[1]==[(2,units('60000.25'),units('1.5'),2)],'publisher decimal BTC')
        check(market(ex.query,2)[1]==[(1,units('2000.50'),units('.12345678'),1)],'publisher decimal ETH')
        # Same-price priority and canceled quantities must survive the planned cut.
        check(rpc(ex.control,'restart 1')['ok'],'planned restart failed')
        snap=alice.snapshot()[a];check(snap[8]==units('.3') and snap[6]==units('.5'),'promotion reduction state')
        carol.order('BTC',1,'60000.25','.6',1);carol.snapshot()
        history=carol.history();check([t[6] for t in history]==[a,a,b],'FIFO changed after promotion')
        # Same-account crossing must reduce both orders without adding trades.
        own=carol.order('BTC',1,'59000.25','.4')
        carol.order('BTC',2,'59000.25','.1')
        state=carol.snapshot()[own]
        check(state[6]==units('.3') and state[8]==units('.1'), 'TCP dc reduction')
        check(len(carol.history())==3,'TCP dc created a trade')
        carol.reduce(own,'0')
        # Persist a reduction on ETH, then kill that active worker and reopen cluster.
        bob.reduce(eth,'.1');state=rpc(ex.control,'status')['result'];active=state['shards']['2']['active']
        key=next(k for k in state['processes'] if '2' in k and active in k and k!='router')
        os.kill(state['processes'][key]['pid'],signal.SIGKILL)
        for c in clients:c.close()
        clients=[];ex.cluster.terminate();ex.cluster.wait(timeout=20)
        ex.cluster=ex.launch(ex.args,'cluster-recovered.log');eventually(ex.ready,'cold recovery failed')
        bob=Client(ex.gateway,'Bob');clients.append(bob)
        snap=bob.snapshot()[eth];check(snap[6]==units('.1') and snap[8]==units('.02345678'),'SIGKILL lost reduction')
        check(len(bob.history())==1,'trade history after recovery')
        # Client-side syntax and display, including instrument discovery and no replay.
        cli=ex.launch([binary/'gateway_client','--port',ex.gateway,'--user','Dave','--password','test'],'cli.log',subprocess.PIPE)
        log=root/'cli.log'
        def plain():return re.sub(r'\x1b\[[0-9;]*m','',log.read_text())
        eventually(lambda:'READY (' in plain(),'CLI login')
        cli.stdin.write(b'instrument ETH\nsell ETH 2500.25 0.12345678 GTC\n');cli.stdin.flush()
        oid=int(eventually(lambda:(re.search(r'ACCEPTED.*order=(\d+)',plain()) or [None,None])[1],'CLI decimal acceptance'))
        cli.stdin.write(f'reduce {oid} 0.1\nrelogin\ncancel-last\nbuy BTC 1 0.1 IOC\nbuy BTC 1.001 0.1\nbuy BTC 1 0.000000001\ninstrument BAD\n'.encode());cli.stdin.flush()
        eventually(lambda:'INPUT_ERROR' in plain() and 'Unknown instrument' in plain(),'CLI validation/unknown instrument')
        check('reason=REDUCED' in plain() and 'canceled=0.02345678' in plain(),'CLI reduction display')
        check('EXPIRED reason=IOC' in plain() and 'price_tick=0.01' in plain(),'CLI IOC/spec display')
        check('REJECTED' in plain(),'CLI off-tick rejection')
        cli.stdin.write(b'quit\n');cli.stdin.flush();cli.wait(timeout=5);check(cli.returncode==0,'CLI exit')
        # Measure shared batch timestamps and ensure the feed remains 200ms.
        ex.udp.setblocking(False)
        try:
            while True:ex.udp.recv(65536)
        except BlockingIOError:pass
        ex.udp.settimeout(5);batches={}
        while len([v for v in batches.values() if len(v)==2])<12:
            data=ex.udp.recv(65536);v,kind,epoch,symbol,index,stamp=MD_HEADER.unpack_from(data)
            check(v==3 and kind in (1,2),'UDP format')
            batches.setdefault((epoch,index),{})[symbol]=stamp
        complete=[(key,stamps) for key,stamps in batches.items() if len(stamps)==2]
        times=[]
        for _,stamps in complete:
            check(stamps[1]==stamps[2],'batch timestamp mismatch');times.append(stamps[1])
        times.sort();intervals=[(b-a)/1e6 for a,b in zip(times,times[1:])]
        check(170<statistics.median(intervals)<260,'publication cadence changed')
        result={'tcp_decimal_ioc_reduce_stp_protocol':3,'planned_restart':'passed','SIGKILL_recovery':'passed','CLI':'passed','median_publication_ms':statistics.median(intervals)}
        (root/'results.json').write_text(json.dumps(result,indent=2));print('PASS',json.dumps(result),'RESULTS',root)
    finally:
        for c in clients:c.close()
        ex.close()

def main():
    parser=argparse.ArgumentParser();parser.add_argument('bin',type=Path);parser.add_argument('output',type=Path);args=parser.parse_args()
    args.output.mkdir(parents=True,exist_ok=True)
    root=Path(tempfile.mkdtemp(prefix='coinbase-',dir=args.output.resolve()))
    exercise(args.bin.resolve(),root)
if __name__=='__main__':main()
