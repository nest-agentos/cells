import {readFile} from 'node:fs/promises';
export const name='cells-session-channel';
export const inject=['tools'];
/** Use the exact native Agent identity; never let model arguments select a scope. */
export async function invoke(file,tool,args,execution,request=globalThis.fetch) {
    const id=execution.agent?.id;
    if(typeof id!=='string')throw new Error('CHANNEL_SESSION_REQUIRED');
    const bindings=await readFile(file,'utf8').then(raw=>JSON.parse(raw)).catch(()=>{throw new Error('CHANNEL_BINDINGS_INVALID');});
    if(!bindings||typeof bindings!=='object')throw new Error('CHANNEL_BINDINGS_INVALID');
    if(!Object.hasOwn(bindings,id))throw new Error('CHANNEL_SESSION_UNBOUND');
    const binding=bindings[id],version='2026-07-28';
    const signal=execution.signal?AbortSignal.any([execution.signal,AbortSignal.timeout(25000)]):AbortSignal.timeout(25000);
    const response=await request(binding.url,{method:'POST',redirect:'error',signal,headers:{...binding.headers,'Content-Type':'application/json','Accept':'application/json',
        'MCP-Protocol-Version':version,'Mcp-Method':'tools/call','Mcp-Name':tool},body:JSON.stringify({jsonrpc:'2.0',id:execution.callId??'cells-channel',method:'tools/call',
            params:{name:tool,arguments:args,_meta:{'io.modelcontextprotocol/protocolVersion':version}}})});
    const value=await response.json().catch(()=>{throw new Error('CHANNEL_RESPONSE_INVALID');});
    if(!value||typeof value!=='object')throw new Error('CHANNEL_RESPONSE_INVALID');
    if(!response.ok)throw new Error(safeCode(value.code,'CHANNEL_HTTP_'+response.status));
    if(value.error||value.result?.isError){const code=value.result?.structuredContent?.code;throw new Error(safeCode(code,'CHANNEL_TOOL_FAILED'));}
    if(!value.result?.structuredContent||typeof value.result.structuredContent!=='object')throw new Error('CHANNEL_RESPONSE_INVALID');
    return value.result.structuredContent;
}
function safeCode(value,fallback){return typeof value==='string'&&/^[A-Z][A-Z0-9_]{0,79}$/.test(value)?value:fallback;}
export function apply(ctx,config) {
    for(const tool of config.tools)ctx.effect(()=>ctx.tools.register({name:'mcp__cells_session__'+tool.name,description:tool.description,parameters:tool.inputSchema,
        output:{schema:{type:'object',additionalProperties:true},render:(_args,value)=>[{type:'text',text:JSON.stringify(value)}]},
        execute:(args,execution)=>invoke(config.bindings,tool.name,args,execution)}),'cells-session:'+tool.name);
}
