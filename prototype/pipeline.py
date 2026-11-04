#!/usr/bin/env python3
"""Riskiest-component prototype: ack-after-durable collector + replayable processor.
Proves: (1) no acked event is lost across a processor crash, (2) duplicates/retries are
counted once, (3) every acked event is accounted for: clean + duplicate + bot + dlq == acked.
Durable log = append-only file, fsync per batch (stand-in for Kinesis). Stdlib only."""
import json,os,sys,time,re,random,hashlib
from datetime import datetime
D=os.path.join(os.path.dirname(os.path.abspath(__file__)),'run');os.makedirs(D,exist_ok=True)
LOG,CK,OUT=f'{D}/raw.log',f'{D}/checkpoint',f'{D}/out.jsonl'
LEGACY={'pageview':'page_view'};EMAIL=re.compile(r'[^@\s"]+@[^@\s"]+\.[a-z]{2,}',re.I);PHONE=re.compile(r'\+?\d[\d\-\s]{7,}\d')
WRITE_KEYS={'wk-042':'t-042','wk-017':'t-017','wk-088':'t-088'}
def now():return datetime.utcnow().isoformat(timespec='milliseconds')+'Z'
class Collector:
    """Returns ack only after the batch is fsync'd. Raw bytes kept verbatim (malformed included)."""
    def __init__(s,batch=500):s.f=open(LOG,'ab');s.buf=[];s.batch=batch;s.acked=0
    def accept(s,raw:str,write_key=None):
        s.buf.append(json.dumps({'raw':raw,'wk':write_key,'received_at':now()}).encode()+b'\n')
        if len(s.buf)>=s.batch:s.flush()
    def flush(s):
        if not s.buf:return
        s.f.write(b''.join(s.buf));s.f.flush();os.fsync(s.f.fileno());s.acked+=len(s.buf);s.buf=[]
def T(x):
    try:return datetime.fromisoformat(x.replace('Z','+00:00'))
    except Exception:return None
class Processor:
    def __init__(s):
        # checkpoint = 'input_offset output_bytes' committed together; on restart, output past the
        # committed point is truncated, so replayed input can never be counted twice.
        s.off,ob=(map(int,open(CK).read().split()) if os.path.exists(CK) else (0,0))
        if os.path.exists(OUT):
            with open(OUT,'r+b') as fo: fo.truncate(ob)
        s.seen=set();s.bursts={};s.c={'clean':0,'duplicate':0,'bot':0,'dlq':0}
        if os.path.exists(OUT):  # rebuild dedupe + counters from committed output (exactly-once effect)
            for l in open(OUT):
                r=json.loads(l);s.c[r['_route']]+=1
                if r.get('_key'):s.seen.add(tuple(r['_key']))
    def run(s,crash_after=None):
        f=open(LOG,'rb');f.seek(s.off);o=open(OUT,'a');n=0
        for line in f:
            env=json.loads(line);r=s.handle(env)
            o.write(json.dumps(r)+'\n');s.c[r['_route']]+=1;n+=1
            s.off+=len(line)
            if n%1000==0:s.commit(o)
            if crash_after and n>=crash_after:return 'CRASHED'   # simulated kill: output since last checkpoint is NOT committed
        s.commit(o);return 'DONE'
    def commit(s,o):
        o.flush();os.fsync(o.fileno());tmp=CK+'.tmp'
        open(tmp,'w').write(f'{s.off} {o.tell()}');os.replace(tmp,CK)   # atomic checkpoint
    def handle(s,env):
        try:e=json.loads(env['raw'])
        except Exception:return {'_route':'dlq','_why':'malformed'}
        t=e.get('tenant_id') or WRITE_KEYS.get(env.get('wk'))
        if not t:return {'_route':'dlq','_why':'no_tenant','event_id':e.get('event_id')}
        e['tenant_id']=t
        if e.get('type') in LEGACY or 'timestamp' in e:
            e['type']=LEGACY.get(e['type'],e['type']);e['ts']=e.pop('timestamp',e.get('ts'))
            p=e.get('properties',{});'page_path' in p and p.__setitem__('path',p.pop('page_path'));'ref' in p and p.__setitem__('referrer',p.pop('ref'))
        k=(t,e.get('event_id'))
        if k in s.seen:return {'_route':'duplicate','_key':None,'event_id':e.get('event_id')}
        s.seen.add(k);e['_key']=list(k);e['received_at']=env['received_at']
        ts,ra=T(e.get('ts') or ''),T(env['received_at'])
        e['event_time']=env['received_at'] if (not ts or (ts-ra).total_seconds()>5) else e['ts']
        props=json.dumps(e.get('properties') or {})
        if EMAIL.search(props) or PHONE.search(props):
            e['properties']={k2:('sha256:'+hashlib.sha256(str(v).encode()).hexdigest()[:16] if EMAIL.search(str(v)) or PHONE.search(str(v)) else v) for k2,v in e['properties'].items()};e['pii_redacted']=True
        (e.get('properties') or {}).pop('count_today',None)  # never trust client-side aggregates
        if e.get('type')=='page_view':
            b=s.bursts.setdefault((t,e.get('anonymous_id')),[]);b.append(ts or ra);b[:]=[x for x in b if (b[-1]-x).total_seconds()<1]
            if len(b)>=3 or 'bot' in str((e.get('properties') or {}).get('referrer','')):e['_route']='bot';return e
        e['_route']='clean';return e
def reset():
    for p in (LOG,CK,OUT):
        if os.path.exists(p):os.remove(p)
