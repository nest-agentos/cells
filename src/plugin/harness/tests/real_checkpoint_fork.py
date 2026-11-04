#!/usr/bin/env python3
"""Opt-in native checkpoint acceptance: isolated state and a local rejecting model peer."""
import http.server, json, os, pathlib, select, shutil, subprocess, tempfile, threading, time
class Reject(http.server.BaseHTTPRequestHandler):
 def do_POST(self):
  self.rfile.read(int(self.headers.get('Content-Length',0)))
  body=b'{"error":{"message":"offline checkpoint fixture","type":"invalid_request_error"}}'
  self.send_response(400); self.send_header('Content-Type','application/json'); self.end_headers(); self.wfile.write(body)
 def log_message(self,*args): pass
with tempfile.TemporaryDirectory(prefix='x1-native-checkpoint-') as directory:
 root=pathlib.Path(directory); home=root/'codex'; workspace=root/'workspace'; home.mkdir(); workspace.mkdir()
 server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Reject); threading.Thread(target=server.serve_forever,daemon=True).start()
 (home/'config.toml').write_text('model_provider="fixture"\nmodel="fixture"\n[model_providers.fixture]\nname="Offline fixture"\nbase_url="http://127.0.0.1:'+str(server.server_port)+'/v1"\nwire_api="responses"\nrequires_openai_auth=false\nrequest_max_retries=0\nstream_max_retries=0\n')
 env={'PATH':os.environ['PATH'],'HOME':str(root),'CODEX_HOME':str(home),'LANG':'C.UTF-8'}
 with (root/'stderr').open('w') as errors:
  p=subprocess.Popen([os.environ.get('CODEX_BINARY') or shutil.which('codex') or 'codex','app-server'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errors,env=env,cwd=workspace,text=True,bufsize=1)
  pending={}; notifications=[]; sequence=0
  def receive(deadline):
   if not select.select([p.stdout],[],[],max(0,deadline-time.monotonic()))[0]: raise RuntimeError('Native protocol timed out')
   line=p.stdout.readline()
   if not line: raise RuntimeError('Native protocol exited')
   value=json.loads(line)
   if 'id' in value and ('result' in value or 'error' in value): pending[value['id']]=value
   else: notifications.append(value)
  def call(method,params):
   global sequence
   sequence+=1; identity=sequence
   p.stdin.write(json.dumps({'id':identity,'method':method,'params':params})+'\n'); p.stdin.flush(); deadline=time.monotonic()+25
   while identity not in pending: receive(deadline)
   value=pending.pop(identity)
   if 'error' in value: raise RuntimeError(method+' rejected with code '+str(value['error'].get('code')))
   return value['result']
  def wait_turn(identity):
   deadline=time.monotonic()+25
   while not any(row.get('method')=='turn/completed' and row.get('params',{}).get('turn',{}).get('id')==identity for row in notifications): receive(deadline)
  try:
   initialized=call('initialize',{'clientInfo':{'name':'x1-checkpoint-test','version':'1'},'capabilities':{'experimentalApi':True}})
   p.stdin.write(json.dumps({'method':'initialized','params':{}})+'\n'); p.stdin.flush()
   started=call('thread/start',{'cwd':str(workspace),'model':'fixture','modelProvider':'fixture','approvalPolicy':'never','sandbox':'read-only'})
   assert started.get('modelProvider')=='fixture', 'Unexpected model provider'
   parent=started['thread']['id']; turns=[]
   for text in ['Offline first checkpoint','Offline second checkpoint']:
    turn=call('turn/start',{'threadId':parent,'input':[{'type':'text','text':text}]})['turn']['id']; wait_turn(turn); turns.append(turn)
   forked=call('thread/fork',{'threadId':parent,'lastTurnId':turns[0],'excludeTurns':True,'cwd':str(workspace),'approvalPolicy':'never','sandbox':'read-only'})['thread']['id']
   copied=call('thread/turns/list',{'threadId':forked,'limit':100,'sortDirection':'desc','itemsView':'notLoaded'})['data']
   original=call('thread/turns/list',{'threadId':parent,'limit':100,'sortDirection':'desc','itemsView':'notLoaded'})['data']
   assert len(copied)==1 and copied[0]['id']==turns[0] and len(original)==2, 'Checkpoint prefix mismatch'
   print(json.dumps({'native':initialized.get('userAgent','').split(' ')[0],'parentTurns':len(original),'forkTurns':len(copied),'exactCheckpoint':True,'modelProvider':'local-rejection-fixture'}))
  finally:
   p.terminate()
   try:p.wait(timeout=5)
   except subprocess.TimeoutExpired:p.kill();p.wait()
   server.shutdown()
