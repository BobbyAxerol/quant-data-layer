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
stream (canonical_record_cap_reached) and in DONE (record_cap_reached).

Launch (OBSERVER_VERSION v3, 2026-10-15): v1/v2 ran the worker by `docker exec` inside query_kn_1, so Query CFS
throttling (0.15-0.49 s per ~12 s in the 2026-10-14 campaign) leaked into the observer receipt clock. v3 runs the same
worker in its own `docker run --rm` container: the image query_kn_1 runs (image id resolved by docker inspect at
launch), the same docker networks, and ONLY the query_kn_1 mounts that hold the TLS files the worker reads, each
re-mounted read-only; explicit --cpus/--memory/--pids-limit, read-only rootfs, no capabilities, a name carrying the
campaign id, and `docker rm -f` of that name on every exit path of the launcher (the worker also ends at its own
--seconds deadline, so a SIGKILLed launcher still leaves a container that exits and is auto-removed). The window
receipt (receipt.json and the leading `launch` line of observations.jsonl) records launch_mode, image id/digests,
cpus and memory. Sampling schedule, record-cap semantics and per-interval keys are unchanged from v2.

Report-only additions in v3 (no verdict uses them): window_coverage() - per venue/feed/interval record counts and
whether the record cap ended the reader before the window end (with the time); produce_split() - per feed/venue
(+interval) venue->ingest (received_at_ns - source), ingest->producer timestamp (Kafka CreateTime - received_at_ns,
ms resolution) and producer timestamp->observer receipt.

Full-window aggregates (OBSERVER_VERSION v4, 2026-10-15, Codex review S100-4): v1-v3 emitted every admitted record and
ended the canonical reader at the first --max-records (10,000) records; BOOK_SNAPSHOT is unsampled, so all five windows
of p184-final-campaign-20261015T134656Z hit the cap after 74-99 s and lost 20.7-86.8 s tails. v4 never stops observing
at a record count. The reader runs to the window end and folds every admitted record (the UNCHANGED v2 sampler: one per
--sample-ms per venue/symbol/feed(+interval), every BOOK_SNAPSHOT) into fixed-memory aggregates per feed|venue|interval
(and the OKX inverse split): count, missing/negative clocks, exact min/max and a fixed log-linear histogram (1 ms
buckets below 128 ms, then 128 sub-buckets per power of two: <= 0.8 % relative width). The stdout carries: bounded
stratified raw samples (--max-records is now only the raw-sample cap: <= RAW_PER_SLICE admitted records per key per
1/RAW_SLICES of the window), a top-OUTLIER_K outlier capsule per key over every accepted event (exact timestamps), and
cumulative `aggregate` lines every CHECKPOINT_SECONDS plus a final one (observed_start_ns/observed_end_ns/
planned_end_ns = the explicit coverage of the whole window). Report-only: all_events_visibility_ms (every accepted
event, not only the admitted ones).

Percentiles from v4 aggregates are BUCKET_UPPER_BOUND (nearest rank as v1-v3, reported as the upper edge of the bucket
holding that rank, with the bucket's lower edge alongside); max is exact. Baseline comparability: the frozen G4 table
and the addendum (config/v2/p184-stage100-g4-addendum.json) are unchanged. The gated population keeps the baseline
admission rule; what changes is (a) coverage: the whole planned window instead of the first 10,000 admitted records
(a capped baseline window measured a prefix), and (b) p95 resolution: an upper bound at most one bucket (<= 0.8 %, or
1 ms below 128 ms) above the exact sample p95, so it can never PASS a row that the exact value fails; a p95 bucket
that straddles a limit is NOT_MEASURED (unresolved), never FAIL or PASS (scripts/p184_gate_campaign.py score_g4).
The addendum rows are scored on max, which stays exact.

Own-cgroup CPU evidence (OBSERVER_VERSION v5, 2026-10-15, G4 common-mode tail RCA): v3/v4 bound the observer with
--cpus but never recorded whether that bound throttled it, so an observer-scheduling share of the receipt clock could be
neither shown nor excluded. v5 reads its OWN container cgroup v2 files (/sys/fs/cgroup/cpu.stat and cpu.max, private
cgroup namespace: the observer container's cgroup only) at READY, at every aggregate checkpoint and at the final
aggregate, and emits them as `observer_cgroup` lines. cgroup_summary() folds them into the receipt (and the campaign's
G4 window receipts) as report-only: window usage in cores, nr_throttled/nr_periods, throttled_ms, and the same per
checkpoint interval. No verdict reads it; sampling, aggregates, percentiles and the frozen baseline are unchanged."""
import argparse
import hashlib
import heapq
import json
import math
from pathlib import Path
import re
import signal
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
OBSERVER_VERSION='qdl.p184.g4-observer.v5'
LAUNCH_MODE='docker_run_isolated'
DEFAULT_CPUS=0.5
DEFAULT_MEMORY='512m'
PIDS_LIMIT=64


def accepted(venue,symbol,feed,symbols,feeds):
    return venue in ('BINANCE','OKX') and feed in feeds and (symbols is None or symbol in symbols)


def summarize(lines):
    """Per feed/venue (BAR per interval): visibility upper bound and venue->DL receipt, ms; missing clocks counted.
    v4 files: from the full-window aggregate (percentiles BUCKET_UPPER_BOUND with their bucket lower edge, exact max,
    report-only all-event visibility and outlier capsule); v1-v3 files: from the raw records, exact sample quantiles."""
    lines=list(lines)
    aggregate=last_aggregate(json.loads(line) for line in lines if line.strip())
    if aggregate is not None:return summarize_aggregate(aggregate)
    return _summarize_records(lines)


def summarize_aggregate(aggregate):
    out={}
    for key,row in sorted((aggregate.get('rows') or {}).items()):
        admitted=row.get('admitted') or {}
        block={name:Histogram.from_json(admitted.get(name)).stats() for name in ('visibility_ms','venue_to_dl_ms')}
        block.update(missing_clock=(row.get('missing_clock') or {}).get('visibility_ms',0),population=POPULATION,
                     all_events_visibility_ms=Histogram.from_json(row.get('all_events_visibility_ms')).stats(),
                     raw_samples=row.get('raw_samples'),
                     outliers=[{k:o.get(k) for k in ('topic','partition','offset','source_event_ns','received_at_ns',
                                                     'producer_timestamp_ms','observer_receipt_wall_ns')}
                               | {'native_symbol':(o.get('identity') or {}).get('native_symbol'),
                                  'visibility_ms':round((o['observer_receipt_wall_ns']-o['source_event_ns'])/1e6,3)
                                  if o.get('observer_receipt_wall_ns') and o.get('source_event_ns') else None}
                               for o in row.get('outliers') or []])
        out[key]=block
    return out


def _summarize_records(lines):
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


def _quantiles(values):
    values=sorted(values)
    if not values:return None
    pick=lambda f:round(values[min(len(values)-1,int(f*len(values)))],1)  # noqa: E731
    return {'n':len(values),'p50':pick(.5),'p95':pick(.95),'max':round(values[-1],1),'negative':sum(1 for x in values if x<0)}


def _row_key(item):
    return '|'.join(str(x) for x in (item.get('feed'),(item.get('identity') or {}).get('venue'),item.get('interval') or ''))


def window_coverage(items):
    """Records per feed|venue|interval and whether the observation ended before the window end.
    v4: explicit coverage from the last aggregate (observed_start_ns .. observed_end_ns against the worker's own
    planned_end_ns; a partial aggregate or an early end is truncated_before_end). v1-v3: planned end = READY at_ns +
    scope seconds; truncated_before_end only when the record-cap line is earlier than that."""
    items=list(items)
    aggregate=last_aggregate(items)
    if aggregate is not None:
        start,end,planned=(aggregate.get(k) for k in ('observed_start_ns','observed_end_ns','planned_end_ns'))
        complete=bool(aggregate.get('final') and end is not None and planned is not None
                      and end>=planned-COVERAGE_TOLERANCE_NS)
        rows=aggregate.get('rows') or {}
        seconds=next((i.get('seconds') for i in items if i.get('kind')=='scope'),None)
        return {'coverage_basis':'FULL_WINDOW_AGGREGATE','observer_version':aggregate.get('observer_version'),
                'per_key_records':{k:((r.get('admitted') or {}).get('visibility_ms') or {}).get('n',0)
                                   +(r.get('missing_clock') or {}).get('visibility_ms',0) for k,r in sorted(rows.items())},
                'per_key_raw_samples':{k:r.get('raw_samples') for k,r in sorted(rows.items())},
                'window_seconds':seconds,'observed_start_ns':start,'observed_end_ns':end,'planned_end_ns':planned,
                'observed_seconds':round((end-start)/1e9,1) if start and end else None,
                'aggregate_final':bool(aggregate.get('final')),'events_accepted':aggregate.get('events_accepted'),
                'events_admitted':aggregate.get('events_admitted'),'record_cap_reached':False,
                'raw_sample_cap_reached':bool(aggregate.get('raw_cap_reached')),
                'truncated_before_end':not complete,'truncated_at_ns':None if complete else end,
                'unobserved_tail_seconds':None if complete or not (planned and end) else round(max(0,planned-end)/1e9,1)}
    per_key={};scope=ready=cap=done=None
    for item in items:
        kind=item.get('kind')
        if kind=='canonical_observation':
            key=_row_key(item);per_key[key]=per_key.get(key,0)+1
        elif kind=='scope':scope=item
        elif kind=='READY':ready=item
        elif kind=='canonical_record_cap_reached':cap=item
        elif kind=='DONE':done=item
    seconds=(scope or {}).get('seconds')
    planned_end=ready['at_ns']+int(seconds*1e9) if ready and seconds else None
    capped=bool(cap or (done or {}).get('record_cap_reached'))
    truncated_at=(cap or {}).get('at_ns')
    truncated=bool(capped and truncated_at is not None and (planned_end is None or truncated_at<planned_end))
    return {'per_key_records':dict(sorted(per_key.items())),'window_seconds':seconds,'planned_end_ns':planned_end,
            'record_cap_reached':capped,'truncated_before_end':truncated,'truncated_at_ns':truncated_at if truncated else None,
            'truncated_after_seconds':(cap or {}).get('after_seconds') if truncated else None,
            'unobserved_tail_seconds':round((planned_end-truncated_at)/1e9,1) if truncated and planned_end else None}


def produce_split(items):
    """Report-only produce-stage split per feed|venue|interval, ms: venue->ingest (received - source),
    ingest->producer timestamp (Kafka CreateTime, ms resolution - received), producer timestamp->observer receipt.
    A missing clock drops that sample from that stage only (counted in missing). v4: from the full-window aggregate
    (BUCKET_UPPER_BOUND percentiles, exact max)."""
    items=list(items)
    aggregate=last_aggregate(items)
    if aggregate is not None:
        out={}
        for key,row in sorted((aggregate.get('rows') or {}).items()):
            admitted=row.get('admitted') or {};missing=row.get('missing_clock') or {}
            names={'venue_to_ingest_ms':'venue_to_dl_ms','ingest_to_producer_ts_ms':'ingest_to_producer_ts_ms',
                   'producer_ts_to_receipt_ms':'producer_ts_to_receipt_ms'}
            out[key]={name:Histogram.from_json(admitted.get(metric)).stats() for name,metric in names.items()}
            out[key]['missing_clock']={name:missing.get(metric) for name,metric in names.items() if missing.get(metric)}
        return out
    stages={};missing={}
    for item in items:
        if item.get('kind')!='canonical_observation':continue
        key=_row_key(item);row=stages.setdefault(key,{'venue_to_ingest_ms':[],'ingest_to_producer_ts_ms':[],'producer_ts_to_receipt_ms':[]})
        source,received,seen=item.get('source_event_ns'),item.get('received_at_ns'),item.get('observer_receipt_wall_ns')
        produced=item.get('producer_timestamp_ms');produced=produced*1_000_000 if produced is not None else None
        for name,lo,hi in (('venue_to_ingest_ms',source,received),('ingest_to_producer_ts_ms',received,produced),('producer_ts_to_receipt_ms',produced,seen)):
            if lo and hi:row[name].append((hi-lo)/1e6)
            else:
                counts=missing.setdefault(key,{});counts[name]=counts.get(name,0)+1
    out={}
    for key,row in sorted(stages.items()):
        out[key]={name:_quantiles(values) for name,values in row.items()}
        out[key]['missing_clock']=missing.get(key,{})
    return out


# ---------------------------------------------------------------- v4 fixed-memory full-window aggregates
LINEAR_MS=128            # 1 ms buckets below this
SUB_BUCKETS=128          # per power of two at and above LINEAR_MS (relative width <= 1/128)
RAW_SLICES=24            # raw samples are stratified over 24 equal slices of the window
RAW_PER_SLICE=4          # per feed|venue|interval and slice
OUTLIER_K=5              # per feed|venue|interval, over every accepted event
CHECKPOINT_SECONDS=30
CGROUP_DIR=Path('/sys/fs/cgroup')  # inside the observer container: its own cgroup v2 (private cgroup namespace)
CGROUP_FIELDS=('usage_usec','user_usec','system_usec','nr_periods','nr_throttled','throttled_usec')
COVERAGE_TOLERANCE_NS=1_500_000_000  # last poll (<= 0.2 s) + final scheduling slack
METRICS=('visibility_ms','venue_to_dl_ms','ingest_to_producer_ts_ms','producer_ts_to_receipt_ms')
PERCENTILE_BASIS='BUCKET_UPPER_BOUND'
POPULATION='ADMITTED_V2_SAMPLER_FULL_WINDOW'
BASELINE_COMPARABILITY=('frozen G4 table and addendum unchanged; gated population = the baseline admission rule '
                        '(v2 sampler, every BOOK_SNAPSHOT) over the whole planned window instead of the first 10,000 '
                        'admitted records; p95 is a bucket upper bound (<= 0.8 % or 1 ms above the exact sample value, a '
                        'straddled limit is unresolved); max exact')


def bucket_index(ms):
    """Fixed log-linear bucket of a duration in ms; -1 = negative (clock skew, counted, never clamped)."""
    if ms<0:return -1
    if ms<LINEAR_MS:return int(ms)
    exponent=int(math.floor(math.log2(ms)))
    while 2.0**(exponent+1)<=ms:exponent+=1
    while 2.0**exponent>ms:exponent-=1
    width=2.0**exponent/SUB_BUCKETS
    return LINEAR_MS+(exponent-7)*SUB_BUCKETS+min(SUB_BUCKETS-1,int((ms-2.0**exponent)//width))


def bucket_bounds(index):
    """[lower, upper) of a bucket in ms; the negative bucket is (None, 0.0)."""
    if index<0:return None,0.0
    if index<LINEAR_MS:return float(index),float(index+1)
    k=index-LINEAR_MS;exponent=7+k//SUB_BUCKETS;width=2.0**exponent/SUB_BUCKETS
    lower=2.0**exponent+(k%SUB_BUCKETS)*width
    return lower,lower+width


class Histogram:
    """Count, exact min/max, negative count and sparse fixed buckets (memory bounded by the bucket scheme)."""
    __slots__=('n','negative','min','max','buckets')

    def __init__(self):
        self.n=0;self.negative=0;self.min=None;self.max=None;self.buckets={}

    def add(self,ms):
        index=bucket_index(ms)
        self.buckets[index]=self.buckets.get(index,0)+1
        self.n+=1;self.negative+=ms<0
        self.min=ms if self.min is None or ms<self.min else self.min
        self.max=ms if self.max is None or ms>self.max else self.max

    def bounds_at(self,fraction):
        """Bucket [lower, upper) holding the nearest-rank sample of v1-v3 (index int(f*n) of the sorted values)."""
        if not self.n:return None
        rank=min(self.n-1,int(fraction*self.n));cumulative=0
        for index in sorted(self.buckets):
            cumulative+=self.buckets[index]
            if cumulative>rank:return bucket_bounds(index)
        return None

    def stats(self):
        out={'n':self.n,'negative':self.negative,'percentile_basis':PERCENTILE_BASIS,
             'max':round(self.max,3) if self.n else None,'min':round(self.min,3) if self.n else None}
        for name,fraction in (('p50',.5),('p95',.95),('p99',.99)):
            bounds=self.bounds_at(fraction) if self.n and (name!='p99' or self.n>=100) else None
            out[name]=round(bounds[1],3) if bounds else None
            out[f'{name}_bucket_lower']=(round(bounds[0],3) if bounds[0] is not None else None) if bounds else None
        if self.n and out['max'] is not None:  # an upper bound never exceeds the exact maximum's own bucket edge
            for name in ('p50','p95','p99'):
                if out[name] is not None and out[name]>out['max']:out[name]=out['max']
        return out

    def to_json(self):
        return {'n':self.n,'negative':self.negative,'min':self.min,'max':self.max,
                'buckets':[[index,count] for index,count in sorted(self.buckets.items())]}

    @classmethod
    def from_json(cls,value):
        h=cls()
        if not value:return h
        h.n=int(value.get('n') or 0);h.negative=int(value.get('negative') or 0)
        h.min=value.get('min');h.max=value.get('max')
        h.buckets={int(i):int(c) for i,c in value.get('buckets') or []}
        if sum(h.buckets.values())!=h.n:raise ValueError('histogram bucket counts differ from n')
        return h


def clock_values(source,received,produced_ms,seen):
    """{metric: ms or None}: the same four quantities v1-v3 derived from a raw record (missing clock -> None)."""
    produced=produced_ms*1_000_000 if produced_ms is not None else None
    pairs={'visibility_ms':(source,seen),'venue_to_dl_ms':(source,received),
           'ingest_to_producer_ts_ms':(received,produced),'producer_ts_to_receipt_ms':(produced,seen)}
    return {name:((hi-lo)/1e6 if lo and hi else None) for name,(lo,hi) in pairs.items()}


class _Row:
    __slots__=('admitted','missing','all_events','all_missing','strata','outliers')

    def __init__(self):
        self.admitted={m:Histogram() for m in METRICS};self.missing={m:0 for m in METRICS}
        self.all_events=Histogram();self.all_missing=0;self.strata={};self.outliers=[]


class WindowAggregator:
    """Pure fixed-memory state of one observer window: per row key aggregates, raw-sample strata, outlier capsule.

    observe() returns the raw record to emit (bounded stratified sample) or None; the caller never caps the reader."""
    def __init__(self,seconds,*,raw_cap=10000,raw_slices=RAW_SLICES,raw_per_slice=RAW_PER_SLICE,outlier_k=OUTLIER_K):
        self.seconds=seconds;self.raw_cap=raw_cap;self.raw_slices=raw_slices;self.raw_per_slice=raw_per_slice
        self.outlier_k=outlier_k;self.rows={};self.inverse={};self.seq=0
        self.start_ns=self.planned_end_ns=self.last_receipt_ns=None
        self.accepted=self.admitted=self.raw_emitted=0;self.raw_cap_reached=False

    def begin(self,start_ns,planned_end_ns=None):
        self.start_ns=start_ns
        self.planned_end_ns=planned_end_ns if planned_end_ns is not None else start_ns+int(self.seconds*1e9)

    def _slice(self,wall_ns):
        if self.start_ns is None:return 0
        span=max(1,(self.planned_end_ns-self.start_ns)//self.raw_slices)
        return max(0,min(self.raw_slices-1,(wall_ns-self.start_ns)//span))

    def observe(self,feed,venue,interval,symbol,source,received,produced_ms,seen,admitted,make_record):
        key='|'.join(str(x) for x in (feed,venue,interval or ''))
        row=self.rows.get(key) or self.rows.setdefault(key,_Row())
        values=clock_values(source,received,produced_ms,seen)
        visibility=values['visibility_ms']
        self.accepted+=1;self.last_receipt_ns=seen
        if visibility is None:row.all_missing+=1
        else:
            row.all_events.add(visibility)
            if len(row.outliers)<self.outlier_k or visibility>row.outliers[0][0]:
                self.seq+=1;item=(visibility,self.seq,make_record())
                if len(row.outliers)<self.outlier_k:heapq.heappush(row.outliers,item)
                else:heapq.heapreplace(row.outliers,item)
        if not admitted:return None
        self.admitted+=1
        targets=[row]
        if symbol.endswith('-USD-SWAP'):  # OKX inverse split of the same quantity (g4_inverse_rows)
            ikey=f'{feed}|OKX_INVERSE|{interval or ""}'
            targets.append(self.inverse.get(ikey) or self.inverse.setdefault(ikey,_Row()))
        for target in targets:
            for name,value in values.items():
                if value is None:target.missing[name]+=1
                else:target.admitted[name].add(value)
        stratum=self._slice(seen)
        if row.strata.get(stratum,0)>=self.raw_per_slice:return None
        if self.raw_emitted>=self.raw_cap:
            self.raw_cap_reached=True;return None
        row.strata[stratum]=row.strata.get(stratum,0)+1;self.raw_emitted+=1
        return make_record()

    @staticmethod
    def _row_json(row,outliers):
        out={'admitted':{m:h.to_json() for m,h in row.admitted.items()},'missing_clock':dict(row.missing)}
        if outliers:
            out.update(all_events_visibility_ms=row.all_events.to_json(),all_events_missing_clock=row.all_missing,
                       raw_samples=sum(row.strata.values()),
                       outliers=[record for _,_,record in sorted(row.outliers,key=lambda x:(-x[0],x[1]))])
        return out

    def snapshot(self,at_ns,observed_end_ns,*,final):
        return {'kind':'aggregate','observer_version':OBSERVER_VERSION,'final':final,'at_ns':at_ns,
                'observed_start_ns':self.start_ns,'observed_end_ns':observed_end_ns,'planned_end_ns':self.planned_end_ns,
                'last_receipt_ns':self.last_receipt_ns,'events_accepted':self.accepted,'events_admitted':self.admitted,
                'raw_emitted':self.raw_emitted,'raw_cap':self.raw_cap,'raw_cap_reached':self.raw_cap_reached,
                'population':POPULATION,'percentile_basis':PERCENTILE_BASIS,
                'histogram':{'linear_ms_below':LINEAR_MS,'sub_buckets_per_octave':SUB_BUCKETS},
                'raw_sampling':{'slices':self.raw_slices,'per_slice_per_key':self.raw_per_slice},
                'outlier_capsule':{'k_per_key':self.outlier_k,'basis':'every accepted event, visibility_ms'},
                'baseline_comparability':BASELINE_COMPARABILITY,
                'rows':{k:self._row_json(r,True) for k,r in sorted(self.rows.items())},
                'inverse_rows':{k:self._row_json(r,False) for k,r in sorted(self.inverse.items())}}


def read_cgroup_cpu(directory=None):
    """The observer's own cgroup v2 cpu.stat counters (CGROUP_FIELDS) and cpu.max bound; None when unreadable."""
    directory=Path(directory or CGROUP_DIR)
    try:
        stat={}
        for line in (directory/'cpu.stat').read_text().splitlines():
            parts=line.split()
            if len(parts)==2 and parts[0] in CGROUP_FIELDS and parts[1].isdigit():stat[parts[0]]=int(parts[1])
        if not {'usage_usec','nr_periods','nr_throttled','throttled_usec'}<=set(stat):return None
        try:
            quota,period=(directory/'cpu.max').read_text().split()[:2]
            stat['cpu_max_cores']=None if quota=='max' else round(int(quota)/int(period),4)
        except (OSError,ValueError):stat['cpu_max_cores']=None
        return stat
    except OSError:
        return None


def cgroup_line(at_ns,label,directory=None):
    """One `observer_cgroup` evidence line (report-only)."""
    return {'kind':'observer_cgroup','at_ns':at_ns,'label':label,'cpu':read_cgroup_cpu(directory),
            'source':'OBSERVER_OWN_CGROUP_V2_CPU_STAT'}


def _cgroup_delta(a,b):
    elapsed=(b['at_ns']-a['at_ns'])/1e9
    x,y=a['cpu'],b['cpu']
    periods=y['nr_periods']-x['nr_periods'];throttled=y['nr_throttled']-x['nr_throttled']
    return {'start_ns':a['at_ns'],'end_ns':b['at_ns'],'seconds':round(elapsed,3),
            'usage_cores':round((y['usage_usec']-x['usage_usec'])/1e6/elapsed,4) if elapsed>0 else None,
            'nr_periods':periods,'nr_throttled':throttled,
            'throttled_ratio':round(throttled/periods,4) if periods>0 else 0.0,
            'throttled_ms':round((y['throttled_usec']-x['throttled_usec'])/1e3,3)}


def cgroup_summary(items):
    """Report-only: the observer's own CPU use and CFS throttling over the window and per checkpoint interval.

    Pure over the `observer_cgroup` lines; never read by a verdict. Pre-v5 windows (no such line) and unreadable
    counters say so instead of guessing."""
    lines=[i for i in items if i.get('kind')=='observer_cgroup']
    if not lines:return {'available':False,'reason':'NO_OBSERVER_CGROUP_LINES (pre-v5 observer)','report_only':True}
    good=sorted((i for i in lines if i.get('cpu')),key=lambda i:i['at_ns'])
    if len(good)<2:return {'available':False,'reason':'FEWER_THAN_TWO_READABLE_SAMPLES','samples':len(lines),
                           'unreadable':len(lines)-len(good),'report_only':True}
    intervals=[_cgroup_delta(a,b) for a,b in zip(good,good[1:])]
    window=_cgroup_delta(good[0],good[-1])
    return {'available':True,'basis':'OBSERVER_OWN_CGROUP_V2_CPU_STAT','report_only':True,
            'cpu_max_cores':good[-1]['cpu'].get('cpu_max_cores'),'samples':len(good),'unreadable':len(lines)-len(good),
            'window':window,'intervals':intervals,
            'max_interval_throttled_ratio':max(i['throttled_ratio'] for i in intervals),
            'max_interval_usage_cores':max((i['usage_cores'] or 0) for i in intervals)}


def last_aggregate(items):
    """The newest final `aggregate` line, else the newest partial one (a killed worker), else None (v1-v3 files)."""
    final=partial=None
    for item in items:
        if item.get('kind')=='aggregate':
            if item.get('final'):final=item
            else:partial=item
    return final or partial


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
    reader=None; sampler=Sampler(args.sample_ms); streak={}; begin=time.monotonic(); end=begin+args.seconds; next_lag=begin
    window=WindowAggregator(args.seconds,raw_cap=args.max_records); last_poll_ns=None; next_checkpoint=None; finished=False
    symbols=set(args.symbols.split(',')) if args.symbols else None
    feeds=set(args.feeds.split(','))
    if args.include_quote:feeds.add('quote')
    emit({'kind':'scope','observer_version':OBSERVER_VERSION,'mode':args.mode,'seconds':args.seconds,'max_records':args.max_records,'filter':{'symbols':sorted(symbols) if symbols else 'ALL','feeds':sorted(feeds),'venues':['BINANCE','OKX']},'sample_ms_per_venue_feed':args.sample_ms,'sampling_key':'venue,symbol,feed (+interval for bar)','snapshot_sampling':'ALL_ADMITTED_FULL_WINDOW','max_records_semantics':'RAW_SAMPLE_CAP_ONLY_READER_NEVER_STOPS','aggregate':{'population':POPULATION,'percentile_basis':PERCENTILE_BASIS,'checkpoint_seconds':CHECKPOINT_SECONDS},'public_top_prices':args.include_top,'kafka_commits':False,'group_joins':False,'production_writes':0,'timestamp_note':'Kafka CreateTime is producer time, never transaction commit time. Observer clocks include fetch/scheduling delay.'})
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
        ready_ns,ready_mono=time.time_ns(),time.monotonic()
        window.begin(ready_ns,ready_ns+int(max(0,end-ready_mono)*1e9));last_poll_ns=ready_ns;next_checkpoint=ready_mono+CHECKPOINT_SECONDS
        emit({'kind':'READY','at_ns':ready_ns,'setup_seconds':ready_mono-begin,'planned_end_ns':window.planned_end_ns})
        emit(cgroup_line(time.time_ns(),'ready'))
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
            if reader and time.monotonic()>=next_checkpoint:
                next_checkpoint+=CHECKPOINT_SECONDS
                emit(window.snapshot(time.time_ns(),last_poll_ns,final=False))
                emit(cgroup_line(time.time_ns(),'checkpoint'))
            if reader:
                message=reader.poll(min(.2,max(0,end-time.monotonic())))
                wall,mono=time.time_ns(),time.monotonic_ns();last_poll_ns=wall
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
                interval=bar_interval(event)
                admitted=sampler.admit(event.venue,event.native_symbol,feed,interval,mono)
                def make_record(message=message,event=event,wall=wall,mono=mono):
                    record=identity_record(message,event,wall,mono)
                    if args.include_top:record['top']=top_record(event)
                    return record
                kind,stamp=message.timestamp()
                raw=window.observe(feed,event.venue,interval,event.native_symbol,event.source_event_time_ns or None,
                                   event.received_at_ns or None,stamp if kind==1 and stamp>=0 else None,wall,admitted,make_record)
                if raw is not None:emit(raw)
            else:time.sleep(min(.1,max(0,end-time.monotonic())))
        emit(window.snapshot(time.time_ns(),last_poll_ns,final=True));finished=True
        emit(cgroup_line(time.time_ns(),'final'))
        emit({'kind':'DONE','at_ns':time.time_ns(),'records':window.raw_emitted,'record_cap_reached':False,'raw_sample_cap_reached':window.raw_cap_reached,'events_accepted':window.accepted,'events_admitted':window.admitted,'max_records':args.max_records,'elapsed_seconds':time.monotonic()-begin,'live_semantics':'observed durable visibility, exact broker commit clock unavailable'})
    finally:
        if reader is not None and not finished and window.start_ns is not None:
            try:
                emit(window.snapshot(time.time_ns(),last_poll_ns,final=False))
                emit(cgroup_line(time.time_ns(),'partial'))
            except Exception:pass
        if reader is not None:reader.close()

def container_name(campaign_id,output):
    """Deterministic, docker-valid name of one window's observer container (campaign id + window directory)."""
    raw=f'qdl-p184-g4obs-{campaign_id}-{Path(output).name}'
    return re.sub(r'[^a-zA-Z0-9_.-]','-',raw).strip('-.')[:120]


def tls_paths(config):
    """Every file path the worker's Kafka clients read (the only container paths it needs)."""
    return sorted({v for role in config.values() for k,v in role.items() if k.endswith('.location')})


def isolated_run_plan(query,paths,name,cpus,memory):
    """Pure: (docker run argv up to the image, provenance) from the query_kn_1 inspect document.

    Same image id and networks as query_kn_1; of its mounts only those holding `paths` (longest destination prefix),
    each re-mounted read-only; unmatched path -> RuntimeError (never guess a host path)."""
    mounts=[]
    for path in paths:
        best=max((m for m in query.get('Mounts') or [] if path==m['Destination'] or path.startswith(m['Destination'].rstrip('/')+'/')),
                 key=lambda m:len(m['Destination']),default=None)
        if best is None:raise RuntimeError(f'no {QUERY} mount holds {path}')
        if best not in mounts:mounts.append(best)
    specs=[]
    for m in sorted(mounts,key=lambda m:m['Destination']):
        if m.get('Type')=='volume':specs.append(f"type=volume,src={m['Name']},dst={m['Destination']},readonly,volume-nocopy")
        elif m.get('Type')=='bind':specs.append(f"type=bind,src={m['Source']},dst={m['Destination']},readonly")
        else:raise RuntimeError(f"unsupported mount type {m.get('Type')} at {m['Destination']}")
    networks=sorted((query.get('NetworkSettings') or {}).get('Networks') or {})
    if not networks:raise RuntimeError(f'{QUERY} has no docker network')
    image=query['Image']
    user=(query.get('Config') or {}).get('User') or None
    argv=['docker','run','--rm','-i','--init','--name',name,'--label','qdl.p184.role=g4-observer',
          '--cpus',f'{cpus:g}','--memory',memory,'--memory-swap',memory,'--pids-limit',str(PIDS_LIMIT),
          '--read-only','--tmpfs','/tmp:rw,noexec,nosuid,size=16m','--cap-drop','ALL','--security-opt','no-new-privileges']
    for network in networks:argv+=['--network',network]
    for spec in specs:argv+=['--mount',spec]
    if user:argv+=['--user',user]
    argv+=['--entrypoint','python',image]
    provenance={'launch_mode':LAUNCH_MODE,'container_name':name,'image_id':image,
                'image_ref':(query.get('Config') or {}).get('Image'),'image_source_container':QUERY,'cpus':cpus,
                'memory':memory,'pids_limit':PIDS_LIMIT,'networks':networks,'user':user,
                'mounts':[{'destination':m['Destination'],'type':m.get('Type'),'readonly':True} for m in mounts]}
    return argv,provenance


def remove_container(name):
    """`docker rm -f` by exact name, then confirm absence (bounded); idempotent after --rm auto-removal."""
    subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30,check=False)
    for _ in range(20):
        present=subprocess.run(['docker','ps','-aq','--filter',f'name=^/{name}$'],capture_output=True,text=True,timeout=10,check=False)
        if present.returncode==0 and not present.stdout.strip():return True
        time.sleep(.5)
    return False


def _terminate(signum,frame):raise SystemExit(128+signum)


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
    query=json.loads(subprocess.check_output(['docker','inspect',QUERY],text=True))[0]
    name=container_name(args.campaign_id or args.output.resolve().parent.name,args.output)
    run_argv,provenance=isolated_run_plan(query,tls_paths(config),name,args.cpus,args.memory)
    image=json.loads(subprocess.check_output(['docker','image','inspect',provenance['image_id']],text=True))[0]
    provenance['image_repo_digests']=image.get('RepoDigests') or []
    command=[*run_argv,'-B','-c',Path(__file__).read_text(),'--worker','--mode',args.mode,'--seconds',str(args.seconds),'--partitions',','.join(map(str,args.partitions)),'--tail-records',str(args.tail_records),'--max-records',str(args.max_records),'--sample-ms',str(args.sample_ms)]
    if args.include_quote:command.append('--include-quote')
    if args.symbols:command += ['--symbols',args.symbols]
    command += ['--feeds',args.feeds]
    if args.include_top:command.append('--include-top')
    launch_record={'kind':'launch','observer_version':OBSERVER_VERSION,'at_ns':time.time_ns(),**provenance}
    previous=signal.signal(signal.SIGTERM,_terminate)
    process=None;written=0;code=None
    try:
        with (args.output/'observations.jsonl').open('x') as evidence:
            line=json.dumps(launch_record,separators=(',',':'))+'\n'
            evidence.write(line);evidence.flush();written+=len(line.encode());print(line.strip(),flush=True)
            process=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,text=True)
            process.stdin.write(json.dumps(config));process.stdin.close()
            for line in process.stdout:
                written+=len(line.encode())
                if written>args.max_output_bytes:
                    raise RuntimeError('Evidence byte cap exceeded; inspect observer completion before further work')
                evidence.write(line);evidence.flush()
                item=json.loads(line)
                if item['kind'] in ('READY','DONE','lag_unknown','canonical_error','canonical_record_cap_reached'):print(line.strip(),flush=True)
        code=process.wait(timeout=15)
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:process.wait(timeout=10)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=10)
        removed=remove_container(name)
        signal.signal(signal.SIGTERM,previous)
    items=[json.loads(line) for line in (args.output/'observations.jsonl').read_text().splitlines() if line.strip()]
    (args.output/'receipt.json').write_text(json.dumps({'exit':code,'observer_version':OBSERVER_VERSION,'output_bytes':written,'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'observer_only':True,'shared_writes':0,'payloads_retained':False,'public_top_prices_retained':args.include_top,
        'launch':provenance,'container_absent_after_exit':removed,'coverage':window_coverage(items),'produce_split':produce_split(items),
        'observer_cgroup':cgroup_summary(items),'report_only':['coverage','produce_split','observer_cgroup']},indent=2)+'\n')
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
    parser.add_argument('--campaign-id',default='',help='container name tag; default: the output parent directory name')
    parser.add_argument('--cpus',type=float,default=DEFAULT_CPUS,help='observer container CPU bound')
    parser.add_argument('--memory',default=DEFAULT_MEMORY,help='observer container memory bound (also memory-swap)')
    args=parser.parse_args();args.partitions=[int(p) for p in args.partitions.split(',')]
    if args.summarize:
        print(json.dumps(summarize(args.summarize.read_text().splitlines()),indent=1));return
    if set(args.feeds.split(','))-set(ALL_FEEDS.split(',')):parser.error('unknown feed')
    if not 1<=args.seconds<=1200 or not 1<=args.max_records<=10000 or not 0<=args.tail_records<=100 or not 0<=args.sample_ms<=1000 or not args.partitions or any(p not in range(6) for p in args.partitions) or not 0.1<=args.cpus<=2 or not re.fullmatch(r'[1-9][0-9]{0,4}m',args.memory):parser.error('Bounded observer arguments violated')
    if args.worker:worker(args)
    else:
        if args.output is None:parser.error('--output is required')
        launch(args)
if __name__=='__main__':main()
