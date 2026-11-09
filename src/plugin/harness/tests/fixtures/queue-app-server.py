#!/usr/bin/env python3
import json,os,sys
state=__STATE__; count=__COUNT__; finish=__FINISH__
thread=json.load(open(state)) if os.path.exists(state) else {'id':'native','cwd':os.getcwd(),'status':{'type':'idle'},'turns':[]}
thread['status']={'type':'notLoaded'}
def emit(value): print(json.dumps(value),flush=True)
def save():
 with open(state,'w') as output: json.dump(thread,output)
for line in sys.stdin:
 request=json.loads(line); method=request.get('method'); params=request.get('params',{})
 if request.get('id') is None: continue
 result={}
 if method in ('thread/start','thread/resume'):
  thread['cwd']=params.get('cwd',thread['cwd']); thread['status']={'type':'idle'}; save();result={'thread':thread}
 elif method=='thread/read':
  if os.path.exists(finish) and thread['turns']:
   os.unlink(finish); thread['turns'][-1]['status']='completed'; thread['status']={'type':'idle'};save();emit({'method':'turn/completed','params':{'threadId':'native','turn':thread['turns'][-1]}})
  result={'thread':dict(thread)}
  if params.get('includeTurns') is False: result['thread']['turns']=[]
 elif method=='thread/turns/list':result={'data':list(reversed(thread['turns'])),'nextCursor':None}
 elif method=='thread/list':result={'data':[thread],'nextCursor':None}
 elif method=='turn/start':
  rows=open(count).readlines() if os.path.exists(count) else []; turn={'id':'turn-'+str(len(rows)+1),'status':'inProgress','items':[]}
  with open(count,'a') as output: output.write(turn['id']+'\n')
  thread['turns'].append(turn);thread['status']={'type':'active'};save();result={'turn':{'id':turn['id']}}
  emit({'id':request['id'],'result':result});emit({'method':'turn/started','params':{'threadId':'native','turn':turn}});continue
 emit({'id':request['id'],'result':result})
