import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { copyFile, cp, mkdtemp, rm, writeFile, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { functionImage } from '../functions/image.js';

const execute = promisify(execFile);
const docker = async (...arguments_: string[]) =>
  (await execute('docker', arguments_, { timeout: 180000, maxBuffer: 1048576 })).stdout;

/** Actual locked LangGraph dependency, OCI adapter ancestry and generated ontology SDK. */
export async function agentImage(options: { childProgram?: string; programSource?: string } = {}) {
  const base = await functionImage();
  const directory = await mkdtemp(join(tmpdir(), 'moria-agent-image-'));
  const repositories: string[] = [];
  const close = async () => {
    for (const repository of repositories)
      await docker('image', 'rm', '--force', repository).catch(() => {});
    await base.close();
    await rm(directory, { recursive: true, force: true });
  };
  const build = async (name: string, source: string) => {
    const repository = base.artifact.uri.slice(6).replace(/\/[^/]+$/, `/${name}`);
    repositories.push(repository);
    await writeFile(join(directory, 'Dockerfile'), source);
    await docker('build', '--network=none', '--pull=false', '--tag', repository, directory);
    const pushed = await docker('push', repository);
    const digest = pushed.match(/digest: (sha256:[0-9a-f]{64})/)?.[1];
    assert.ok(digest);
    return { uri: `oci://${repository}`, digest };
  };
  try {
    for (const name of ['package.json', 'package-lock.json', 'adapter.mjs'])
      await copyFile(
        new URL(`../../../workers/agents/${name}`, import.meta.url),
        join(directory, name),
      );
    for (const name of ['package.json', 'package-lock.json'])
      assert.ok(
        (await readFile(join(directory, name))).equals(await readFile('/opt/moria-agent/' + name)),
        'Locked Agent dependencies are stale; rebuild the test image',
      );
    await cp('/opt/moria-agent/node_modules', join(directory, 'node_modules'), {
      recursive: true,
      verbatimSymlinks: true,
    });
    const adapter = await build(
      'moria-agent-adapter',
      `FROM ${base.artifact.uri.slice(6)}@${base.artifact.digest}\nUSER root\nWORKDIR /agent\nCOPY package.json package-lock.json ./\nCOPY node_modules/ ./node_modules/\nCOPY adapter.mjs ./adapter.mjs\nUSER 65532:65532\nCMD ["node","/function/runtime/runner.mjs","/agent/adapter.mjs"]\n`,
    );
    await rm(join(directory, 'node_modules'), { recursive: true });
    const programSource =
      options.programSource ??
      `import {StateGraph,Annotation,START,END} from '@langchain/langgraph';
export {createClient as createOsdk} from '/function/sdk-js/index.js';
export function createGraph(context){
const state=Annotation.Root({mode:Annotation(),prompt:Annotation(),uid:Annotation(),answer:Annotation()});
return new StateGraph(state).addNode('decision',async input=>{
  if(input.mode==='hold'){await new Promise((resolve,reject)=>{const timer=setTimeout(resolve,120000);context.signal.addEventListener('abort',()=>{clearTimeout(timer);reject(new Error('cancelled'));},{once:true});});}
  if(input.mode==='sleep')await new Promise(resolve=>setTimeout(resolve,10000));
  if(input.mode==='fail')throw new Error('deterministic graph failure');
  if(input.mode==='normal'||input.mode==='sleep')return{answer:input.prompt};
  if(input.mode.startsWith('pending-child-')){
    const response=await context.platform.fetch('/api/v1/functions/'+input.prompt+'/invoke',{method:'POST',headers:{'content-type':'application/json','idempotency-key':'pending-child'},body:JSON.stringify({input:{mode:'child',value:1,uid:input.uid},wait:false})});
    if(!response.ok)throw new Error('child admission denied');
    let child=await response.json();
    if(input.mode==='pending-child-progress'){
      while(['accepted','queued','running','preparing'].includes(child.status)){
        context.signal.throwIfAborted();
        await new Promise(resolve=>setTimeout(resolve,100));
        const next=await context.platform.fetch('/api/v1/invocations/'+child.id);
        if(!next.ok)throw new Error('child progress denied');
        child=await next.json();
      }
      return{answer:String(child.status)};
    }
    await new Promise(resolve=>setTimeout(resolve,10000));
    if(input.mode==='pending-child-log')context.log('pending child artifact must not escape');
    else if(input.mode==='pending-child-action')await context.osdk.actions['edit-record']({uid:input.uid,value:83},{idempotencyKey:'pending-child-effect'});
    else if(input.mode==='pending-child-model')await context.model.chat({messages:[{role:'user',content:'pending child must not reach provider'}],maxTokens:32},'pending-child-effect');
    else throw new Error('unknown pending child probe');
    return{answer:'effect escaped'};
  }
  if(input.mode==='child'||input.mode==='child-fail'||input.mode==='child-hold'){
    let response=await context.platform.fetch('/api/v1/functions/'+input.prompt+'/invoke',{method:'POST',headers:{'content-type':'application/json','idempotency-key':'agent-child'},body:JSON.stringify({input:{mode:input.mode,value:1,uid:input.uid},wait:true})});
    let outcome=await response.json();
    while(response.ok&&['accepted','queued','running','preparing'].includes(outcome.status)){
      context.signal.throwIfAborted();
      await new Promise(resolve=>setTimeout(resolve,100));
      response=await context.platform.fetch('/api/v1/invocations/'+outcome.id);
      outcome=await response.json();
    }
    return{answer:response.ok?String(outcome.status):'child-denied'};
  }
  let prompt=input.prompt;
  if(input.mode==='read-model'||input.mode==='write-fail')prompt=String((await context.osdk.objects.records.get(input.uid)).object.value);
  const response=await context.model.chat({messages:[{role:'user',content:prompt}],maxTokens:32},'decision');
  if(input.mode==='write-fail'){await context.osdk.actions['edit-record']({uid:input.uid,value:43},{idempotencyKey:'agent-write'});throw new Error('graph failed after committed action');}
  context.log('agent model observation completed');return{answer:response.content};
}).addEdge(START,'decision').addEdge('decision',END).compile();}
export function result(state){return{answer:state.answer};}
`;
    await writeFile(join(directory, 'program.mjs'), programSource);
    const artifact = await build(
      'moria-agent-program',
      `FROM ${adapter.uri.slice(6)}@${adapter.digest}\nCOPY program.mjs /agent/program.mjs\n`,
    );
    const childSource =
      options.childProgram ??
      `
export {createClient as createOsdk} from '/function/sdk-js/index.js';
export default async function(input,context){
  const value=(await context.osdk.objects.records.get(input.uid)).object.value;
  if(input.mode==='child-fail')throw new Error('child failed after read');
  context.log('child observed protected record');
  if(input.mode==='child-hold')await new Promise((resolve,reject)=>{const timer=setTimeout(resolve,60000);context.signal.addEventListener('abort',()=>{clearTimeout(timer);reject(new Error('cancelled'));},{once:true});});
  return{value};
}
`;
    await writeFile(join(directory, 'child.mjs'), childSource);
    const child = await build(
      'moria-agent-child',
      `FROM ${base.artifact.uri.slice(6)}@${base.artifact.digest}\nCOPY child.mjs /function/child.mjs\nCMD ["node","/function/runtime/runner.mjs","/function/child.mjs"]\n`,
    );
    return { adapter, artifact, child, programSource, childSource, close };
  } catch (error) {
    await close();
    throw error;
  }
}
