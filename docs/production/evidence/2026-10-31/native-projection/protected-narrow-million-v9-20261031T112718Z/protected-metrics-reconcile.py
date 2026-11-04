from pathlib import Path
from collections import Counter
import json,hashlib,math
base=Path('/tmp/moria-reconcile-20261029/docs/production/evidence/2026-10-31/native-projection')
def distribution(values):
 values=sorted(values)
 def percentile(p):return values[min(len(values)-1,math.ceil(len(values)*p)-1)]if values else None
 return {'count':len(values),'p50Ms':percentile(.5),'p95Ms':percentile(.95),'p99Ms':percentile(.99),'maxMs':values[-1]if values else None}
def summarize(items):
 items=[dict(sample)for sample in items]
 for sample in items:
  if sample['successful']and sample['completedMs']-sample['scheduledMs']>10000:sample['successful']=False;sample['error']='ARRIVAL_DEADLINE'
 good=[s for s in items if s['successful']];bad=[s for s in items if not s['successful']]
 return {'scheduled':len(items),'successful':len(good),'unsuccessfulFraction':len(bad)/len(items)if items else None,'latencyBasis':'all-scheduled-arrivals','allLatency':distribution([s['completedMs']-s['scheduledMs']for s in items]),'successfulLatency':distribution([s['completedMs']-s['scheduledMs']for s in good]),'dispatchDelay':distribution([s['dispatchMs']-s['scheduledMs']for s in items if'dispatchMs'in s]),'notDispatched':sum('dispatchMs'not in s for s in items),'statuses':dict(Counter(str(s['status'])for s in items if'status'in s)),'errors':dict(Counter(s.get('error','undefined')for s in bad))}
for name in ['header-window-million-v8-20261031T091215Z','protected-narrow-million-v9-20261031T112718Z']:
 e=base/name;p=e/'population-1000000';report={'classification':'postprocessed-unchanged-arrival-records','deadlineMs':10000,'percentileMethod':'nearest rank: ceil(n*p)-1, null on empty; original driver summaries remain unchanged','sourceFiles':{},'windows':[]}
 for rate in [20,50,100]:
  f=p/f'http-{rate}-arrivals.json';data=json.loads(f.read_text());report['sourceFiles'][f.name]=hashlib.file_digest(f.open('rb'),'sha256').hexdigest();report['windows'].append({'rate':rate,'startedAt':data['startedAt'],'total':summarize(data['samples']),'classes':{kind:summarize([s for s in data['samples']if s['kind']==kind])for kind in ['detail','page','filterSort','action','multiAction','aggregate']}})
 (e/'http-successful-metrics.json').write_text(json.dumps(report,indent=2)+'\n');print(name,json.dumps([{'rate':w['rate'],'successful':w['total']['successful'],'successfulLatency':w['total']['successfulLatency'],'dispatchDelay':w['total']['dispatchDelay']}for w in report['windows']]))
 import shutil
 shutil.copyfile(__file__,e/'protected-metrics-reconcile.py')
