#!/usr/bin/env python3
"""Engineer 004 fixture anomaly detector. Usage: python3 detect_anomalies.py event_sample.jsonl
Classifies every line; prints one finding per anomaly with event_id and the pipeline action."""
import json,sys,re,hashlib
from datetime import datetime
P=sys.argv[1] if len(sys.argv)>1 else 'event_sample.jsonl'
raw=open(P,'rb').read();print('fixture sha256:',hashlib.sha256(raw).hexdigest())
def T(s):
    try:return datetime.fromisoformat(s.replace('Z','+00:00'))
    except Exception:return None
EMAIL=re.compile(r'[^@\s"]+@[^@\s"]+\.[a-z]{2,}',re.I);PHONE=re.compile(r'\+?\d[\d\-\s]{7,}\d')
LEGACY={'pageview':'page_view'};KNOWN={'page_view','click','form_submit','identify','custom','privacy_request'}
seen={};F=[];ev=[]
for n,line in enumerate(raw.decode().splitlines(),1):
    try:e=json.loads(line)
    except Exception as x:F.append(('MALFORMED_JSON',re.search(r'evt-\d+',line).group(0) if re.search(r'evt-\d+',line) else f'line{n}','DLQ raw bytes; alert; never drop silently',str(x)[:40]));continue
    ev.append(e);i=e.get('event_id')
    if not e.get('tenant_id'):F.append(('MISSING_TENANT',i,'quarantine; resolve from write key at edge, never guess',''))
    k=(e.get('tenant_id'),i)
    if k in seen:F.append(('DUPLICATE',i,'idempotent dedupe on (tenant_id,event_id); keep first',f"resent {e.get('received_at')}"))
    seen[k]=1
    if 'timestamp' in e or e.get('type') in LEGACY or 'page_path' in (e.get('properties') or {}):
        F.append(('LEGACY_SCHEMA',i,'adapter maps pageview->page_view, timestamp->ts, page_path->path, ref->referrer; stamp received_at server-side',''))
    elif e.get('type') not in KNOWN:F.append(('UNKNOWN_TYPE',i,'DLQ',e.get('type')))
    ts,ra=T(e.get('ts') or e.get('timestamp') or ''),T(e.get('received_at') or '')
    if ts and ra:
        d=(ts-ra).total_seconds()
        if d>86400:F.append(('FUTURE_TIMESTAMP',i,'clamp event time to received_at; keep client ts as attribute',f'+{d/86400:.0f}d'))
        elif d>5:F.append(('CLOCK_SKEW',i,'order/window by received_at; store skew',f'client ahead {d:.0f}s'))
    props=json.dumps(e.get('properties') or {})
    if EMAIL.search(props) or PHONE.search(props):F.append(('PII_IN_PROPERTIES',i,'hash/redact at ingest before any store or stream',''))
    if e.get('type')=='privacy_request':F.append(('PRIVACY_DELETE',i,f"tombstone user {e.get('user_id')} + all stitched anon ids; purge stores and warehouse exports",''))
    if 'count_today' in (e.get('properties') or {}):F.append(('CLIENT_AGGREGATE',i,'ignore client count; recompute server-side',''))
# bot burst: same anon, >=3 page_views within 1s, or scanner referrer
from collections import defaultdict
g=defaultdict(list)
for e in ev:
    if e.get('type')=='page_view':g[(e.get('tenant_id'),e.get('anonymous_id'))].append(e)
for k,L in g.items():
    L.sort(key=lambda e:e['ts'])
    if len(L)>=3 and (T(L[-1]['ts'])-T(L[0]['ts'])).total_seconds()<1:
        F.append(('BOT_BURST',','.join(e['event_id'] for e in L),'flag is_bot; exclude from dashboards/segments, retain raw',f'{len(L)} pages in <1s from {k[1]}'))
# identity stitching
for e in ev:
    if e.get('type')=='identify':F.append(('IDENTITY_STITCH',e['event_id'],f"merge {e['anonymous_id']} -> {e['user_id']} (retroactive for earlier anon events)",''))
for f in F:print(f'{f[0]:18} {f[1]:28} {f[2]}' + (f'  [{f[3]}]' if f[3] else ''))
print(f'\n{len(ev)} parsed, {len(F)} findings')
