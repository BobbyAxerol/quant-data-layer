#!/usr/bin/env python3
"""Explicitly launched, bounded Kafka lag/assigned-tail observer; no writes/joins (P18.4 G4 baseline/candidate).

Origin: ~/.local/state/qdl-v2/g4-trade-mark-20261009/observer.py (sha256 66ce6625...), which hard-coded BTC/ETH TRADE/MARK.
This tracked copy takes the symbol/feed filter as arguments (default: every symbol and feed) and adds `--summarize`,
an offline per feed/venue (BAR per interval) p50/p95/max/n of the visibility upper bound
(observer read_committed receipt - source event time) and of venue -> DL receipt. The observer joins no group,
commits nothing and keeps no payload values.

Sampling (OBSERVER_VERSION v2, 2026-10-14): one record per --sample-ms per (venue, symbol, feed), and for BAR per
(venue, symbol, feed, interval), so bars of different intervals closing in the same second are all kept (v1 keyed BAR
without the interval and kept only the first of e.g. 1m/3m/5m/15m/30m closing together). BOOK_SNAPSHOT stays
unsampled until the global --max-records cap; reaching the cap ends the canonical reader and is reported in the
stream (canonical_record_cap_reached) and in DONE (record_cap_reached)."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
from decimal import Decimal

PROJECTOR='qdl_v2_stable_candidate-market_projector_1-1'
QUERY='qdl_v2_stable_candidate-query_kn_1-1'
GROUPS=[('core','qdl-v2-realtime-core-v2'),('projector','kn-projector-v3-a'),('projector','kn-projector-v3-b'),('producer','qdl-v2-book-resync-ingestor_binance_usdm-binance-public-book-001'),('producer','qdl-v2-book-resync-ingestor_okx_swap-okx-public-001')]

def bounded_decimal(value):
    coefficient=value.WhichOneof('coefficient')
    if coefficient is None or abs(value.scale)>30:return None
    raw=str(getattr(value,coefficient))
    if len(raw)>64 or not raw.lstrip('-').isdigit():return None
    number=Decimal(raw)
    parts=number.as_tuple()
    result=format(Decimal((parts.sign,parts.digits,-value.scale)), 'f')
    return result if len(result)<=96 else None

def top_record(event):
    feed=event.WhichOneof('payload')
    result={'bid_price':None,'ask_price':None,'basis':'UNAVAILABLE_SAMPLED_DELTA_NOT_RECONSTRUCTED'}
    if feed=='quote':
        result.update(bid_price=bounded_decimal(event.quote.bid_price),ask_price=bounded_decimal(event.quote.ask_price),basis='QUOTE_EVENT')
    elif feed=='book_snapshot':
        sides={1:[],2:[]}
        for level in event.book_snapshot.levels:
            price,quantity=bounded_decimal(level.price),bounded_decimal(level.quantity)
            if level.side in sides and price is not None and quantity is not None and Decimal(quantity)>0:
                sides[level.side].append(price)
        result.update(bid_price=max(sides[1],key=Decimal) if sides[1] else None,ask_price=min(sides[2],key=Decimal) if sides[2] else None,basis='SNAPSHOT_EVENT_POSITIVE_QUANTITY_LEVELS')
    return result

ALL_FEEDS='trade,quote,mark_index_price,book_delta,book_snapshot,bar'
OBSERVER_VERSION='qdl.p184.g4-observer.v2'


def accepted(venue,symbol,feed,symbols,feeds):
    return venue in ('BINANCE','OKX') and feed in feeds and (symbols is None or symbol in symbols)


def summarize(lines):
    """Per feed/venue (BAR per interval): visibility upper bound and venue->DL receipt, ms; missing clocks counted."""
    def q(values,f):
        values=sorted(values);return round(values[min(len(values)-1,int(f*len(values)))],1) if values else None
    groups={};missing={}
    for line in lines:
        item=json.loads(line)
        if item.get('kind')!='canonical_observation':continue
        key='|'.join(str(x) for x in (item['feed'],item['identity']['venue'],item.get('interval') or ''))
        source,received,seen=item.get('source_event_ns'),item.get('received_at_ns'),item.get('observer_receipt_wall_ns')
        if not source or not seen or not received:
            missing[key]=missing.get(key,0)+1;continue
        g=groups.setdefault(key,{'visibility_ms':[],'venue_to_dl_ms':[]})
        g['visibility_ms'].append((seen-source)/1e6);g['venue_to_dl_ms'].append((received-source)/1e6)
    out={}
    for key,g in sorted(groups.items()):
        out[key]={name:{'n':len(v),'p50':q(v,.5),'p95':q(v,.95),'p99':q(v,.99) if len(v)>=100 else None,'max':round(max(v),1),
                        'negative':sum(1 for x in v if x<0)} for name,v in g.items()}
        out[key]['missing_clock']=missing.get(key,0)
    for key,count in missing.items():
        out.setdefault(key,{'missing_clock':count})
    return out


def should_emit(feed,mono,last,interval_ms):
    return feed=='book_snapshot' or mono-last>=interval_ms*1_000_000


def bar_interval(event):
    return (str(getattr(event.bar,'interval','')) or None) if event.WhichOneof('payload')=='bar' else None


def sampling_key(venue,symbol,feed,interval=None):
    """BAR is sampled per interval; every other feed keeps the v1 key (venue, symbol, feed)."""
    return (venue,symbol,feed,interval) if feed=='bar' else (venue,symbol,feed)


class Sampler:
    """Per-key rate limit of the canonical reader (pure; the worker's only sampling decision)."""
    def __init__(self,sample_ms):
        self.sample_ms=sample_ms;self.last={}

    def admit(self,venue,symbol,feed,interval,mono):
        key=sampling_key(venue,symbol,feed,interval)
        if not should_emit(feed,mono,self.last.get(key,0),self.sample_ms):return False
        self.last[key]=mono;return True

def identity_record(message,event,wall,mono):
    kind,stamp=message.timestamp()
    return {'kind':'canonical_observation','topic':message.topic(),'partition':message.partition(),'offset':message.offset(),
        'event_id_sha256':hashlib.sha256(bytes(event.event_id)).hexdigest() if event.event_id else None,
        'identity':{k:getattr(event,k) for k in ('instrument_uid','venue','market','product_type','native_symbol','source_id','source_session_id','connection_generation','source_sequence','partition_sequence','authority_revision','partition_plan_epoch')},
        'feed':event.WhichOneof('payload'),'source_event_ns':event.source_event_time_ns or None,
        'received_at_ns':event.received_at_ns or None,'normalized_at_ns':event.normalized_at_ns or None,'published_at_ns':event.published_at_ns or None,
        'producer_timestamp_ms':stamp if kind==1 and stamp>=0 else None,'kafka_timestamp_type':kind,
        'observer_receipt_wall_ns':wall,'observer_receipt_monotonic_ns':mono,
        'boundary':'DURABLE_READ_COMMITTED_OBSERVATION_NOT_COMMIT_CLOCK',
        'interval':bar_interval(event)}

def worker(args):
    from confluent_kafka import Consumer,ConsumerGroupTopicPartitions,TopicPartition,KafkaError,IsolationLevel
    from confluent_kafka.admin import AdminClient,OffsetSpec
    from qdl.marketdata.v2.market_data_pb2 import EventEnvelope
    config=json.load(sys.stdin)
    def emit(value):print(json.dumps(value,separators=(',',':')),flush=True)
    clients={k:AdminClient(v) for k,v in config.items()} if args.mode in ('lag','both') else {}
    reader=None; count=0; sampler=Sampler(args.sample_ms); capped=False; seen={}; streak={}; begin=time.monotonic(); end=begin+args.seconds; next_lag=begin
    symbols=set(args.symbols.split(',')) if args.symbols else None
    feeds=set(args.feeds.split(','))
    if args.include_quote:feeds.add('quote')
    emit({'kind':'scope','observer_version':OBSERVER_VERSION,'mode':args.mode,'seconds':args.seconds,'max_records':args.max_records,'filter':{'symbols':sorted(symbols) if symbols else 'ALL','feeds':sorted(feeds),'venues':['BINANCE','OKX']},'sample_ms_per_venue_feed':args.sample_ms,'sampling_key':'venue,symbol,feed (+interval for bar)','snapshot_sampling':'ALL_UNTIL_GLOBAL_CAP','public_top_prices':args.include_top,'kafka_commits':False,'group_joins':False,'production_writes':0,'timestamp_note':'Kafka CreateTime is producer time, never transaction commit time. Observer clocks include fetch/scheduling delay.'})
    try:
        if args.mode in ('canonical','both'):
            c=dict(config['projector'])
            c.update({'group.id':'kn-projector-v3-a','client.id':'dr07-assigned-readonly-observer','enable.auto.commit':False,'enable.auto.offset.store':False,'isolation.level':'read_committed','auto.offset.reset':'error','enable.partition.eof':True,'queued.max.messages.kbytes':1024,'fetch.max.bytes':1048576,'max.partition.fetch.bytes':262144,'fetch.wait.max.ms':100})
            reader=Consumer(c); assignments=[]
            for partition in args.partitions:
                low,high=reader.get_watermark_offsets(TopicPartition('md.canonical.v2',partition),timeout=3)
                offset=max(low,high-args.tail_records)
                assignments.append(TopicPartition('md.canonical.v2',partition,offset))
                emit({'kind':'assignment','topic':'md.canonical.v2','partition':partition,'offset':offset,'low':low,'high':high,'no_subscription':True})
            reader.assign(assignments)
        emit({'kind':'READY','at_ns':time.time_ns(),'setup_seconds':time.monotonic()-begin})
        while time.monotonic()<end:
            if clients and time.monotonic()>=next_lag:
                next_lag+=10
                for role,group in GROUPS:
                    started=time.monotonic_ns()
                    try:
                        result=clients[role].list_consumer_group_offsets([ConsumerGroupTopicPartitions(group)],request_timeout=3)[group].result(4)
                        offsets=result.topic_partitions
                        if len(offsets)>36:raise ValueError('partition roster exceeded bound')
                        future=clients[role].list_offsets({TopicPartition(p.topic,p.partition):OffsetSpec.latest() for p in offsets},isolation_level=IsolationLevel.READ_UNCOMMITTED,request_timeout=3)
                        records=[]
                        for p in offsets:
                            key=(group,p.topic,p.partition)
                            e=future[TopicPartition(p.topic,p.partition)].result(4).offset
                            valid=p.error is None and p.offset>=0 and e>=p.offset
                            lag=e-p.offset if valid else None
                            prior=streak.get(key)
                            rises=prior[2]+1 if valid and prior and prior[0] is not None and lag>prior[0] and started-prior[1]<=15_000_000_000 else 0
                            streak[key]=(lag,started,rises)
                            records.append({'topic':p.topic,'partition':p.partition,'committed':p.offset,'log_end':e,'lag_records':lag,'consecutive_increases':rises,'five_increases_alarm':rises>=5,'valid':valid})
                        emit({'kind':'lag','at_ns':time.time_ns(),'group':group,'elapsed_ns':time.monotonic_ns()-started,'end_basis':'LOG_END_READ_UNCOMMITTED_NOT_LSO','rows':records})
                    except Exception as exc:
                        for key in list(streak):
                            if key[0]==group:del streak[key]
                        error=exc.args[0] if exc.args else None
                        emit({'kind':'lag_unknown','at_ns':time.time_ns(),'group':group,'error_type':type(exc).__name__,'error_code':error.code() if hasattr(error,'code') else None})
            if reader and count<args.max_records:
                message=reader.poll(min(.2,max(0,end-time.monotonic())))
                wall,mono=time.time_ns(),time.monotonic_ns()
                if message is None:continue
                if message.error():
                    if message.error().code()!=KafkaError._PARTITION_EOF:emit({'kind':'canonical_error','code':message.error().code(),'at_ns':wall})
                    continue
                if message.value() is None:continue
                try:event=EventEnvelope.FromString(message.value())
                except Exception as exc:
                    emit({'kind':'decode_error','topic':message.topic(),'partition':message.partition(),'offset':message.offset(),'error_type':type(exc).__name__});continue
                feed=event.WhichOneof('payload')
                if not accepted(event.venue,event.native_symbol,feed,symbols,feeds):continue
                if not sampler.admit(event.venue,event.native_symbol,feed,bar_interval(event),mono):continue
                record=identity_record(message,event,wall,mono)
                if args.include_top:record['top']=top_record(event)
                emit(record);count+=1
                if count>=args.max_records:
                    reader.close();reader=None;capped=True
                    emit({'kind':'canonical_record_cap_reached','records':count,'at_ns':time.time_ns(),'after_seconds':time.monotonic()-begin})
                    if not clients:break
            else:time.sleep(min(.1,max(0,end-time.monotonic())))
        emit({'kind':'DONE','at_ns':time.time_ns(),'records':count,'record_cap_reached':capped,'max_records':args.max_records,'elapsed_seconds':time.monotonic()-begin,'live_semantics':'observed durable visibility, exact broker commit clock unavailable'})
    finally:
        if reader is not None:reader.close()

def launch(args):
    if not args.launch:raise SystemExit('STAGED ONLY: parent coordination required; add --launch explicitly.')
    args.output.mkdir(parents=True,exist_ok=False)
    current=json.loads(subprocess.check_output(['docker','inspect',PROJECTOR],text=True))[0]
    env=dict(item.split('=',1) for item in current['Config']['Env'] if '=' in item)
    p={k:env[v] for k,v in {'bootstrap.servers':'QDL_KN_KAFKA_BOOTSTRAP','ssl.ca.location':'QDL_KN_KAFKA_CA','ssl.certificate.location':'QDL_KN_KAFKA_CERT','ssl.key.location':'QDL_KN_KAFKA_KEY'}.items()}
    p.update({'security.protocol':'ssl','socket.timeout.ms':3000,'log_level':0})
    config={'projector':p}
    for role in ('core','producer'):
        config[role]={**p,'ssl.ca.location':f'/stable-certs/{role}/ca.crt','ssl.certificate.location':f'/stable-certs/{role}/client.crt','ssl.key.location':f'/stable-certs/{role}/client.key'}
    command=['docker','exec','-i',QUERY,'python','-B','-c',Path(__file__).read_text(),'--worker','--mode',args.mode,'--seconds',str(args.seconds),'--partitions',','.join(map(str,args.partitions)),'--tail-records',str(args.tail_records),'--max-records',str(args.max_records),'--sample-ms',str(args.sample_ms)]
    if args.include_quote:command.append('--include-quote')
    if args.symbols:command += ['--symbols',args.symbols]
    command += ['--feeds',args.feeds]
    if args.include_top:command.append('--include-top')
    process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True)
    process.stdin.write(json.dumps(config));process.stdin.close()
    written=0
    with (args.output/'observations.jsonl').open('x') as evidence:
        for line in process.stdout:
            written+=len(line.encode())
            if written>args.max_output_bytes:
                process.terminate();raise RuntimeError('Evidence byte cap exceeded; inspect observer completion before further work')
            evidence.write(line);evidence.flush()
            item=json.loads(line)
            if item['kind'] in ('READY','DONE','lag_unknown','canonical_error','canonical_record_cap_reached'):print(line.strip(),flush=True)
    code=process.wait(timeout=15)
    (args.output/'receipt.json').write_text(json.dumps({'exit':code,'observer_version':OBSERVER_VERSION,'output_bytes':written,'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'observer_only':True,'shared_writes':0,'payloads_retained':False,'public_top_prices_retained':args.include_top},indent=2)+'\n')
    if code:raise SystemExit(code)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode',choices=['lag','canonical','both'],default='both')
    parser.add_argument('--seconds',type=int,default=60)
    parser.add_argument('--partitions',default='0,1,2,3,4,5')
    parser.add_argument('--tail-records',type=int,default=16)
    parser.add_argument('--max-records',type=int,default=2000)
    parser.add_argument('--sample-ms',type=int,default=100)
    parser.add_argument('--include-quote',action='store_true')
    parser.add_argument('--include-top',action='store_true')
    parser.add_argument('--max-output-bytes',type=int,default=8*1024*1024)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--launch',action='store_true')
    parser.add_argument('--worker',action='store_true',help=argparse.SUPPRESS)
    parser.add_argument('--symbols',default='',help='comma list of native symbols; empty = all')
    parser.add_argument('--feeds',default=ALL_FEEDS,help='comma list of canonical payload kinds')
    parser.add_argument('--summarize',type=Path,help='offline: summarize an observations.jsonl and exit')
    args=parser.parse_args();args.partitions=[int(p) for p in args.partitions.split(',')]
    if args.summarize:
        print(json.dumps(summarize(args.summarize.read_text().splitlines()),indent=1));return
    if set(args.feeds.split(','))-set(ALL_FEEDS.split(',')):parser.error('unknown feed')
    if not 1<=args.seconds<=1200 or not 1<=args.max_records<=10000 or not 0<=args.tail_records<=100 or not 0<=args.sample_ms<=1000 or not args.partitions or any(p not in range(6) for p in args.partitions):parser.error('Bounded observer arguments violated')
    if args.worker:worker(args)
    else:
        if args.output is None:parser.error('--output is required')
        launch(args)
if __name__=='__main__':main()
