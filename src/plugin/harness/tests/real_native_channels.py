#!/usr/bin/env python3
"""Installed Claude/DSH acceptance against one Cells channel, using local fixtures only."""
import sys
sys.dont_write_bytecode=True
from real_claude import EventObserver
import argparse, http.server, json, os, pathlib, shutil, socket, subprocess, tempfile, threading, time, urllib.request, uuid

class Model(http.server.BaseHTTPRequestHandler):
    def log_message(self,*args): pass
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers.get('Content-Length','0'))))
        if 'count_tokens' in self.path:
            self.send_response(200); self.send_header('Content-Type','application/json'); self.end_headers(); self.wfile.write(b'{"input_tokens":1}'); return
        has_result=any(p.get('type')=='tool_result' for m in body.get('messages',[])[-1:] for p in m.get('content',[]) if isinstance(p,dict))
        content={'type':'text','text':'Channel proof complete'} if has_result else {'type':'tool_use','id':'tool-channel-proof','name':'mcp__cells_session__session_report','input':{'id':'native-proof','message':{'type':'progress','content':'Native channel proof'}}}
        message={'id':'msg_'+str(uuid.uuid4()),'type':'message','role':'assistant','model':body.get('model'),'content':[],'stop_reason':None,'stop_sequence':None,'usage':{'input_tokens':1,'output_tokens':0}}
        initial={**content,**({'text':''} if has_result else {'input':{}})}
        frames=[('message_start',{'type':'message_start','message':message}),('content_block_start',{'type':'content_block_start','index':0,'content_block':initial}),('content_block_delta',{'type':'content_block_delta','index':0,'delta':{'type':'text_delta','text':content['text']} if has_result else {'type':'input_json_delta','partial_json':json.dumps(content['input'])}}),('content_block_stop',{'type':'content_block_stop','index':0}),('message_delta',{'type':'message_delta','delta':{'stop_reason':'end_turn' if has_result else 'tool_use','stop_sequence':None},'usage':{'output_tokens':1}}),('message_stop',{'type':'message_stop'})]
        self.send_response(200); self.send_header('Content-Type','text/event-stream'); self.end_headers()
        for event,value in frames:self.wfile.write(f'event: {event}\ndata: {json.dumps(value)}\n\n'.encode()); self.wfile.flush()

def main():
    kind=argparse.ArgumentParser(); kind.add_argument('harness',choices=['claude','dsh']); selected=kind.parse_args().harness
    executable=shutil.which(selected); assert executable,'Harness executable missing'
    binary=pathlib.Path(os.environ.get('CELLS_BINARY',str(pathlib.Path(__file__).resolve().parents[4]/'target/debug/cells'))).resolve()
    with tempfile.TemporaryDirectory(prefix='cells-native-channel-') as temporary:
        root=pathlib.Path(temporary); home=root/'native'; home.mkdir(); work=root/'files'/'work'; work.mkdir(parents=True)
        model=http.server.ThreadingHTTPServer(('127.0.0.1',0),Model); threading.Thread(target=model.serve_forever,daemon=True).start()
        with socket.socket() as listener:listener.bind(('127.0.0.1',0)); port=listener.getsockname()[1]
        base=f'http://127.0.0.1:{port}'; token='fixture-token-0123456789abcdef'; config=root/'cells.toml'
        config.write_text(f'listen="127.0.0.1:{port}"\nexecution=true\nserver_id="native-channel-test"\ntoken="{token}"\n[projects]\nroot={json.dumps(str(root/"catalog"))}\nnetwork=true\nisolation="none"\n[[exports]]\nalias="files"\npath={json.dumps(str(root/"files"))}\naccess="rw"\n[[harnesses]]\nid="{selected}"\nkind="{selected}"\ncommand={json.dumps(executable)}\nhome={json.dumps(str(home))}\nprojects=true\n')
        env={'PATH':os.environ['PATH'],'HOME':str(root),'LANG':'C.UTF-8','ANTHROPIC_API_KEY':'local-fixture-key','ANTHROPIC_BASE_URL':f'http://127.0.0.1:{model.server_port}','DEEPSEEK_API_KEY':'local-fixture-key','DEEPSEEK_BASE_URL':f'http://127.0.0.1:{model.server_port}/v1'}
        log=(root/'cells.log').open('w+'); cells=subprocess.Popen([str(binary),str(config)],env=env,stdout=log,stderr=log)
        def request(path,data=None):
            req=urllib.request.Request(base+path,data=None if data is None else json.dumps(data).encode(),headers={'Authorization':'Bearer '+token,'Content-Type':'application/json'})
            with urllib.request.urlopen(req,timeout=130) as response:return json.load(response)
        def api(domain,operation,args):
            result=request('/v1/api/'+domain+'/'+operation,args)
            if not result.get('ok'):raise RuntimeError(str(result.get('error',{})))
            return result['value']
        try:
            for _ in range(100):
                try:caps=request('/v1/capabilities');break
                except OSError:
                    if cells.poll() is not None:raise RuntimeError('CELLS_START_FAILED')
                    time.sleep(.05)
            project=api('projects','register',{'name':'Probe','alias':'files','path':'','access':'rw'})['project']; binding={'projectId':project['id'],'revision':project['revision']}
            def scope(identity):return api('projects','channel_open',{**binding,'scopeId':identity,'endpoint':base+'/v1/channel/mcp','cwd':f'/projects/{project["id"]}/work','snapshot':{'task':{'title':identity}}})
            scope('first'); epoch=api('harness','profiles',{})['epoch']
            def mutate(operation,args):
                value=api('harness',operation,{**binding,'profileId':selected,'epoch':epoch,'requestId':str(uuid.uuid4()),**args})
                if value.get('outcome')!='committed':raise RuntimeError(str(value))
                return value['result']
            session=mutate('create',{'workspaceId':project['id'],'directory':'work'})['session']; session_id=session['id']
            assert pathlib.Path(session['cwd'])==work,(session['cwd'],str(work))
            if selected=='claude':
                status=json.loads(mutate('command',{'sessionId':session_id,'command':'session:mcp'})['output'])
                assert any(row.get('name')=='cells_session' and row.get('status')=='connected' for row in status),status
                observer=EventObserver(port,{**binding,'profileId':selected})
                mutate('turn',{'sessionId':session_id,'prompt':'Publish the native channel proof through session_report.'})
                for _ in range(200):
                    for pending in observer.pages.get('harness',{}).get('requests',[]):
                        if pending.get('params',{}).get('threadId')==session_id:mutate('respond',{'nativeRequestId':pending['id'],'response':{'decision':'accept'}})
                    messages=api('projects','channel_messages',{**binding,'scopeId':'first'})
                    if any(row['id']=='native-proof' for row in messages['messages']):break
                    time.sleep(.1)
                assert any(row['id']=='native-proof' for row in messages['messages']), 'NATIVE_REPORT_MISSING'
            else:
                directory=next(home.glob('.cells-channel-*')); module=directory/'channel.mjs'; bindings=directory/'bindings.json'
                runner=root/'invoke.mjs'; runner.write_text(f'import {{invoke}} from {json.dumps(module.as_uri())};\nconst result=await invoke({json.dumps(str(bindings))},"session_report",{{id:"native-proof",message:{{type:"progress",content:"DSH channel proof"}}}},{{agent:{{id:{json.dumps(session_id)}}},signal:new AbortController().signal}});if(!result.accepted)throw Error("REPORT_REJECTED");')
                subprocess.run(['node',str(runner)],env={'PATH':os.environ['PATH'],'HOME':str(root)},check=True,timeout=10)
                messages=api('projects','channel_messages',{**binding,'scopeId':'first'});assert messages['messages'][0]['id']=='native-proof'
            for _ in range(200):
                messages=api('projects','channel_messages',{**binding,'scopeId':'first'})
                if messages['execution']['status'] in ['idle','completed']:break
                time.sleep(.1)
            api('projects','channel_close',{**binding,'scopeId':'first','requireIdle':True,'expectedExecutionRevision':messages['execution']['revision']})
            scope('second')
            mutate('resume',{'sessionId':session_id})
            if selected=='dsh':
                subprocess.run(['node',str(runner)],check=True,timeout=10)
            else:
                mutate('turn',{'sessionId':session_id,'prompt':'Publish the native channel proof through session_report.'})
                handled=set()
                for _ in range(200):
                    for pending in observer.pages.get('harness',{}).get('requests',[]):
                        if pending['id'] not in handled and pending.get('params',{}).get('threadId')==session_id:
                            mutate('respond',{'nativeRequestId':pending['id'],'response':{'decision':'accept'}});handled.add(pending['id'])
                    messages=api('projects','channel_messages',{**binding,'scopeId':'second'})
                    if any(row['id']=='native-proof' for row in messages['messages']):break
                    time.sleep(.1)
            messages=api('projects','channel_messages',{**binding,'scopeId':'second'})
            assert any(row['id']=='native-proof' for row in messages['messages']),'ROTATED_NATIVE_REPORT_MISSING'
            print(json.dumps({'rotation':True,'harness':selected,'workingDirectory':True,'nativeSession':True,'sharedChannelReport':True,'externalModelCalls':False}))
        except Exception:
            log.flush(); log.seek(0); lines=log.read().splitlines(); print(json.dumps({'harness':selected,'diagnosticTail':lines[-8:]})); raise
        finally:
            cells.terminate()
            try:cells.wait(timeout=15)
            except subprocess.TimeoutExpired:cells.kill();cells.wait()
            model.shutdown();log.close()
if __name__=='__main__':main()
