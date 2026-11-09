import {afterEach,test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {invoke,apply} from '../../src/plugin/harness/drivers/dsh/channel.mjs';
const directories=[];afterEach(async()=>{for(const path of directories.splice(0))await rm(path,{recursive:true,force:true});});
async function fixture(){const path=await mkdtemp(join(tmpdir(),'dsh-channel-'));directories.push(path);const file=join(path,'bindings.json');await writeFile(file,JSON.stringify({first:{url:'http://localhost/channel',headers:{Authorization:'Bearer first'}},second:{url:'http://localhost/channel',headers:{Authorization:'Bearer second'}}}),{mode:0o600});return file;}
test('uses the actual executing Agent and refreshes its binding without reassigning another Session',async()=>{
 const file=await fixture(),seen=[];
 const request=async(_url,init)=>{seen.push(init);return Response.json({result:{structuredContent:{accepted:true}}});};
 const exec={agent:{id:'first'},callId:'call',signal:new AbortController().signal};
 assert.deepEqual(await invoke(file,'session_report',{id:'report',message:{type:'progress',content:'done'},sessionId:'second'},exec,request),{accepted:true});
 assert.equal(seen[0].headers.Authorization,'Bearer first');assert.equal(seen[0].headers['MCP-Protocol-Version'],'2026-07-28');
 await writeFile(file,JSON.stringify({first:{url:'http://localhost/channel',headers:{Authorization:'Bearer rotated'}}}));
 await invoke(file,'session_context',{},exec,request);assert.equal(seen[1].headers.Authorization,'Bearer rotated');
 await assert.rejects(invoke(file,'session_context',{}, {...exec,agent:{id:'second'}},request),/CHANNEL_SESSION_UNBOUND/);
});
test('requires a bound Agent and preserves typed channel refusals',async()=>{
 const file=await fixture();await assert.rejects(invoke(file,'session_context',{},{}),/CHANNEL_SESSION_REQUIRED/);
 const exec={agent:{id:'first'},signal:new AbortController().signal};
 await assert.rejects(invoke(file,'session_context',{},exec,async()=>Response.json({code:'EUNAUTHENTICATED'},{status:401})),/EUNAUTHENTICATED/);
 await assert.rejects(invoke(file,'session_context',{},exec,async()=>Response.json({code:'private data'},{status:503})),/CHANNEL_HTTP_503/);
 await assert.rejects(invoke(file,'session_context',{},exec,async()=>Response.json({result:{isError:true,structuredContent:{code:'CHANNEL_SCOPE_CLOSED'}}})),/CHANNEL_SCOPE_CLOSED/);
});
test('registers the shared tool schemas and removes registrations with the plugin scope',()=>{
 const registered=[],disposals=[];let removed=0;
 apply({tools:{register:tool=>{registered.push(tool);return()=>removed++;}},effect:start=>disposals.push(start())},{bindings:'/private/bindings',tools:[{name:'session_context',description:'Read',inputSchema:{type:'object'}}]});
 assert.equal(registered[0].name,'mcp__cells_session__session_context');assert.deepEqual(registered[0].parameters,{type:'object'});
 disposals.forEach(dispose=>dispose());assert.equal(removed,1);
});

test('corrupt bindings never disclose file contents through native errors',async()=>{
 const file=await fixture();await writeFile(file,'{"Authorization":"private-token",');
 await assert.rejects(invoke(file,'session_context',{}, {agent:{id:'first'}}),error=>error.message==='CHANNEL_BINDINGS_INVALID');
});
