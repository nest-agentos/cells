#!/usr/bin/env python3
"""Native Codex + Cells MCP 2.0 acceptance without any external model call or account credentials."""
import http.server, json, os, pathlib, queue, socket, sqlite3, subprocess, tempfile, threading, time, urllib.request
binary = pathlib.Path(os.environ.get('CELLS_BINARY', 'tools/cells/target/debug/cells')).resolve()
with tempfile.TemporaryDirectory(prefix='cells-native-channel-') as directory:
 root=pathlib.Path(directory); exports=root/'files'; exports.mkdir(); work=exports/'work'; work.mkdir(); home=root/'codex'; home.mkdir()
 listener=socket.socket(); listener.bind(('127.0.0.1',0)); port=listener.getsockname()[1]; listener.close(); base=f'http://127.0.0.1:{port}'
 token='native-channel-owner-0123456789'
 config=root/'config.toml'; config.write_text(f'listen="127.0.0.1:{port}"\nexecution=false\nserver_id="native-channel-test"\ntoken="{token}"\n[projects]\nroot={json.dumps(str(root/"catalog"))}\nnetwork=true\n[[exports]]\nalias="files"\npath={json.dumps(str(exports))}\naccess="rw"\n')
 def api(operation,args):
  request=urllib.request.Request(base+'/v1/api/projects/'+operation,data=json.dumps(args).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
  with urllib.request.urlopen(request,timeout=8) as response:value=json.load(response)
  assert value['ok'],value.get('error');return value['value']
 cells=subprocess.Popen([str(binary),str(config)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 native=None
 try:
  deadline=time.monotonic()+12
  while True:
   try:
    request=urllib.request.Request(base+'/v1/capabilities',headers={'Authorization':'Bearer '+token})
    with urllib.request.urlopen(request,timeout=1) as response:assert json.load(response)['node']['api']['version']==1
    break
   except Exception:
    if cells.poll() is not None or time.monotonic()>deadline:raise RuntimeError('CELLS_START_FAILED')
    time.sleep(.05)
  project=api('register',{'name':'Probe','alias':'files','path':'','access':'rw','mounts':[]})['project']
  project_id=project['id'];binding={'projectId':project_id,'revision':project['revision']};cwd=f'/projects/{project_id}/work'
  def open_scope(scope,title):
   api('channel_open',{**binding,'scopeId':scope,'cwd':cwd,'endpoint':base+'/v1/channel/mcp','snapshot':{'task':{'title':title}}})
   with sqlite3.connect(root/'catalog'/'session-channels.sqlite') as database:secret=database.execute('SELECT token FROM scopes WHERE project=? AND id=?',(project_id,scope)).fetchone()[0]
   return {'features.mcp_2026_07_28':True,'mcp_servers.cells_session':{'url':base+'/v1/channel/mcp','required':True,'http_headers':{'Authorization':'Bearer '+secret}}}
  channel_config=open_scope('first','First task')
  fixture_calls=[]
  class RejectModel(http.server.BaseHTTPRequestHandler):
   def do_POST(self):
    self.rfile.read(int(self.headers.get('Content-Length',0)));fixture_calls.append(1)
    body=b'{"error":{"message":"offline acceptance fixture","type":"invalid_request_error"}}'
    self.send_response(400);self.send_header('Content-Type','application/json');self.end_headers();self.wfile.write(body)
   def log_message(self,*args):pass
  model=http.server.ThreadingHTTPServer(('127.0.0.1',0),RejectModel);threading.Thread(target=model.serve_forever,daemon=True).start()
  (home/'config.toml').write_text('model_provider="fixture"\nmodel="fixture"\n[model_providers.fixture]\nname="Offline fixture"\nbase_url="http://127.0.0.1:'+str(model.server_port)+'/v1"\nwire_api="responses"\nrequires_openai_auth=false\nrequest_max_retries=0\nstream_max_retries=0\n')
  env={'PATH':os.environ['PATH'],'HOME':str(root),'CODEX_HOME':str(home),'LANG':'C.UTF-8'}
  native=subprocess.Popen([os.environ.get('CODEX_BINARY','codex'),'app-server','-c','thread_unload_delay_secs=0'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,env=env,cwd=work,text=True,bufsize=1)
  messages=queue.Queue()
  def read_native():
   for line in native.stdout:messages.put(json.loads(line))
  threading.Thread(target=read_native,daemon=True).start()
  sequence=0;notifications=[]
  def call(method,params):
   global sequence
   sequence+=1;request_id=sequence;native.stdin.write(json.dumps({'id':request_id,'method':method,'params':params})+'\n');native.stdin.flush();deadline=time.monotonic()+20
   while time.monotonic()<deadline:
    try:value=messages.get(timeout=max(.01,deadline-time.monotonic()))
    except queue.Empty:raise RuntimeError('NATIVE_RPC_TIMEOUT')
    if value.get('method'):notifications.append(value)
    if value.get('id')==request_id:
     if 'error' in value:
      import re
      raise RuntimeError(method+' rejected '+str(value['error'].get('code'))+' '+re.sub(r'[a-f0-9]{64}','<redacted>',str(value['error'].get('message','')))[:250])
     return value['result']
   raise RuntimeError('NATIVE_RPC_TIMEOUT')
  call('initialize',{'clientInfo':{'name':'cells-native-channel-test','version':'1'},'capabilities':{'experimentalApi':True}})
  native.stdin.write('{"method":"initialized","params":{}}\n');native.stdin.flush()
  thread=call('thread/start',{'cwd':str(work),'approvalPolicy':'never','sandbox':'read-only','modelProvider':'fixture','model':'fixture','config':channel_config})['thread']['id']
  def tool(name,args):return call('mcpServer/tool/call',{'threadId':thread,'server':'cells_session','tool':name,'arguments':args})
  statuses=call('mcpServerStatus/list',{'threadId':thread,'limit':100})['data'];assert len(next(s for s in statuses if s['name']=='cells_session')['tools'])==5
  context=tool('session_context',{'section':'task'});assert context['structuredContent']['title']=='First task',context
  report={'id':'progress-one','message':{'type':'progress','content':'Native transport verified'}}
  assert tool('session_report',report)['structuredContent']['accepted']
  assert len(api('channel_messages',{**binding,'scopeId':'first'})['messages'])==1
  assert tool('session_report',report)['structuredContent']['accepted']
  assert tool('session_report',{'id':'progress-one','message':{'type':'progress','content':'Changed'}})['isError']
  request={'id':'context-one','method':'context.probe','params':{}}
  assert tool('session_request',request)['structuredContent']['status']=='pending'
  pending=api('channel_requests',{**binding,'scopeId':'first'})['requests'][0]
  api('channel_respond',{**binding,'scopeId':'first','id':'context-one','requestDigest':pending['requestDigest'],'status':'ok','payload':{'version':1},'error':None})
  assert tool('session_response',{'id':'context-one'})['structuredContent']['payload']['version']==1
  turn=call('turn/start',{'threadId':thread,'input':[{'type':'text','text':'Persist the isolated offline acceptance checkpoint.'}]})['turn']['id']
  deadline=time.monotonic()+15
  while not any(value.get('method')=='turn/completed' and value.get('params',{}).get('turn',{}).get('id')==turn for value in notifications):
   try:notifications.append(messages.get(timeout=max(.01,deadline-time.monotonic())))
   except queue.Empty:raise RuntimeError('NATIVE_TURN_TIMEOUT')
  api('channel_close',{**binding,'scopeId':'first'})
  second_config=open_scope('second','Second task')
  call('thread/unsubscribe',{'threadId':thread})
  deadline=time.monotonic()+10
  while not any(value.get('method')=='thread/closed' and value.get('params',{}).get('threadId')==thread for value in notifications):
   try:notifications.append(messages.get(timeout=max(.01,deadline-time.monotonic())))
   except queue.Empty:raise RuntimeError('NATIVE_UNLOAD_TIMEOUT')
  call('thread/resume',{'threadId':thread,'cwd':str(work),'excludeTurns':True,'config':second_config})
  assert tool('session_context',{'section':'task'})['structuredContent']['title']=='Second task'
  assert any(row['id']==turn for row in call('thread/turns/list',{'threadId':thread,'limit':10,'sortDirection':'desc','itemsView':'notLoaded'})['data'])
  print(json.dumps({'native':'codex','protocol':'2026-07-28','toolCount':5,'checks':['fixed-api','native-context','native-report','immutable-id','host-response','same-session-scope-switch'],'externalModelCalls':0,'fixtureModelCalls':len(fixture_calls)}))
 finally:
  for process in [native,cells]:
   if process is None:continue
   process.terminate()
   try:process.wait(timeout=4)
   except subprocess.TimeoutExpired:process.kill();process.wait()
