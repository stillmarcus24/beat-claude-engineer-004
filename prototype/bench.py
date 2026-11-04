#!/usr/bin/env python3
"""Two runs. A: the challenge fixture end to end. B: synthetic load with a 10x spike,
2% client retries, 1% malformed, bot bursts, and a processor crash mid-stream.
Pass condition for both: clean + duplicate + bot + dlq == acked (nothing unaccounted)."""
import json,time,random,sys,os
sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)));import pipeline as P
from datetime import datetime,timedelta
def recon(tag,acked,c):
    tot=sum(c.values());ok=tot==acked
    print(f'[{tag}] acked={acked} clean={c["clean"]} duplicate={c["duplicate"]} bot={c["bot"]} dlq={c["dlq"]} accounted={tot} -> {"BALANCED" if ok else "MISMATCH"}');return ok
# A: fixture
P.reset();col=P.Collector(batch=1)
fx=os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','event_sample.jsonl')
for l in open(fx):col.accept(l.rstrip('\n'))
col.flush();pr=P.Processor();pr.run();a=recon('fixture',col.acked,pr.c)
# B: load
random.seed(7);P.reset();col=P.Collector(batch=500);base=datetime(2026,6,15,14,0,0);sent=0
def ev(i,t):
    return json.dumps({'event_id':f'e{i}','tenant_id':f't-{i%500:03d}','anonymous_id':f'a{i%20000}','user_id':None,'type':random.choice(['page_view','click','form_submit']),'ts':(base+timedelta(milliseconds=t)).isoformat()+'Z','properties':{'path':'/pricing'}})
N_STEADY,N_SPIKE=100_000,100_000   # spike phase = same count at 10x arrival rate (modelled as ts spacing)
t0=time.perf_counter();i=0;ms=0
for phase,n,step in (('steady',N_STEADY,10),('spike',N_SPIKE,1)):
    for _ in range(n):
        i+=1;ms+=step;r=ev(i,ms);col.accept(r);sent+=1
        x=random.random()
        if x<0.02:col.accept(r)                       # client retry -> duplicate
        elif x<0.03:col.accept(r[:-3])                # truncated payload -> malformed
        if i%5000==0:                                 # bot burst: 4 page views < 50ms same anon
            for k in range(4):col.accept(json.dumps({'event_id':f'b{i}-{k}','tenant_id':'t-001','anonymous_id':f'bot{i}','type':'page_view','ts':(base+timedelta(milliseconds=ms+k*10)).isoformat()+'Z','properties':{'path':'/pricing'}}))
col.flush();ingest=time.perf_counter()-t0
pr=P.Processor();t1=time.perf_counter();st=pr.run(crash_after=123_457);print('processor run 1:',st,'checkpoint offset',pr.off)
pr2=P.Processor();st2=pr2.run();proc=time.perf_counter()-t1
print('processor run 2 (restart from checkpoint):',st2)
b=recon('load+crash',col.acked,pr2.c)
print(f'ingest: {col.acked} records in {ingest:.2f}s = {col.acked/ingest:,.0f} rec/s (fsync per 500)')
print(f'process incl. crash+replay: {proc:.2f}s = {col.acked/proc:,.0f} rec/s')
print('host:',os.uname().nodename,os.cpu_count(),'vCPU')
sys.exit(0 if a and b else 1)
