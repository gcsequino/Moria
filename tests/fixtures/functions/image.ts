import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, writeFile, rm, readdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { writeSdk } from '../../../packages/osdk/src/generate.js';

const exec = promisify(execFile);
const docker = async (args: string[]) =>
  (await exec('docker', args, { timeout: 120000, maxBuffer: 1048576 })).stdout;
export const registryImage =
  'registry@sha256:d4032d4cc323c1731c707931ecada07a53076eac38e3b61ae57533f170f2c6c3';
export const nodeImage =
  'node:24-bookworm-slim@sha256:a5f4366e834695d5f363b920c104953a535d3a9864d1bdf74566a0ebe01daee5';

/** Real OCI manifest publication; the test daemon needs both pinned prerequisite images. */
export async function functionImage() {
  const directory = await mkdtemp(join(tmpdir(), 'moria-function-image-')),
    registry = `moria-function-registry-${randomUUID()}`;
  let repository: string | undefined;
  const close = async () => {
    if (repository) await docker(['image', 'rm', '--force', repository]).catch(() => {});
    await docker(['rm', '--force', registry]).catch(() => {});
    await rm(directory, { recursive: true, force: true });
  };
  try {
    const proof = process.env.MORIA_TEST_PROOF_ID;
    if (proof && !/^[a-z0-9-]{1,128}$/.test(proof)) throw new Error('Invalid proof identity');
    await docker([
      'run',
      '--detach',
      '--rm',
      '--name',
      registry,
      ...(proof ? ['--label', `moria.test.proof=${proof}`] : []),
      '--publish',
      '127.0.0.1::5000',
      registryImage,
    ]);
    const address = (await docker(['port', registry, '5000/tcp'])).trim();
    repository = `${address}/moria-function-fixture`;
    await exec(process.execPath, [
      'scripts/build-function-runtime.mjs',
      join(directory, 'runtime'),
    ]);
    const sdk = join(directory, 'sdk');
    await writeSdk(
      {
        version: 1,
        app: 'functions',
        types: {},
        objects: {
          records: {
            id: '00000000-0000-4000-8000-000000000004',
            type: 'Record',
            properties: {
              uid: { type: 'uid', nullable: false },
              value: { type: 'float64', nullable: false },
            },
          },
        },
        actions: {
          'edit-record': {
            inputs: {
              uid: { type: 'uid', nullable: false },
              value: { type: 'float64', nullable: false },
            },
          },
          'reject-record': {
            inputs: {
              uid: { type: 'uid', nullable: false },
              value: { type: 'float64', nullable: false },
            },
          },
        },
        endpoints: {},
      },
      sdk,
    );
    await exec(
      'pnpm',
      [
        'exec',
        'tsc',
        '--ignoreConfig',
        '--target',
        'ES2024',
        '--module',
        'NodeNext',
        '--moduleResolution',
        'NodeNext',
        '--skipLibCheck',
        '--outDir',
        join(directory, 'sdk-js'),
        ...(await readdir(sdk))
          .filter((name) => name.endsWith('.ts'))
          .map((name) => join(sdk, name)),
      ],
      { timeout: 30000, maxBuffer: 1048576 },
    );
    await writeFile(
      join(directory, 'function.ts'),
      `
import {readFile,writeFile} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {createClient} from './sdk-js/index.js';
import type {Record as OntologyRecord} from './sdk-js/index.js';
export const createOsdk=createClient;
export default async function(input:{mode:string;value:number;uid:string|null;payload?:unknown;customer?:OntologyRecord;customers?:OntologyRecord[]},context:any) {
  context.log('function started');
  if(Object.keys(input).length===0) return {};
  if(input.mode==='echo') return {payload:input.payload};
  if(input.mode==='objects') {
    const value=(await input.customer!.get(['value'])).object.value;
    return Promise.resolve({customer:input.customer,customers:input.customers,value});
  }
  if(input.mode==='nan') return {value:NaN};
  if(input.mode==='undefined') return {value:undefined};
  if(input.mode==='bad') return {value:'wrong type'};
  if(input.mode==='throw') throw new Error('private worker exception');
  if(input.mode==='sleep') await new Promise(resolve=>setTimeout(resolve,10000));
  if(input.mode==='hold') await new Promise((resolve,reject)=>{const timer=setTimeout(resolve,60000);context.signal.addEventListener('abort',()=>{clearTimeout(timer);reject(new Error('cancelled'));},{once:true});});
  if(input.mode==='spin') while(true) {}
  if(input.mode==='oom') {const bytes=Buffer.alloc(512*1024*1024,1); return {value:bytes.length};}
  if(input.mode==='pids') {
    const denied=await Promise.all(Array.from({length:80},()=>new Promise(resolve=>execFile('sleep',['1'],error=>resolve(error?.code==='EAGAIN')))));
    return {value:denied.some(Boolean)?1:0};
  }
  if(input.mode==='sdk-read') return {value:(await context.osdk.objects.records.get(input.uid)).object.value};
  if(input.mode==='platform') {
    const response=await context.platform.fetch('/api/management/v1/fs/home');
    if(!response.ok) throw new Error('platform home failed');
    return {value:(await response.json()).home.kind==='directory'?1:0};
  }
  if(input.mode==='action'||input.mode==='action-fail'||input.mode==='proposal'||input.mode==='proposal-rollback') {
    await context.osdk.objects.records.get(input.uid);
    await context.osdk.actions['edit-record']({uid:input.uid,value:input.value},{idempotencyKey:'edit'});
    if(input.mode==='proposal-rollback') await context.osdk.actions['reject-record']({uid:input.uid,value:999},{idempotencyKey:'reject'});
    if(input.mode==='action-fail') throw new Error('after committed call');
    return {value:input.value};
  }
  if(input.mode==='nested') {
    const response=await context.platform.fetch('/api/v1/functions/'+input.uid+'/invoke',{method:'POST',headers:{'content-type':'application/json','idempotency-key':'child'},body:JSON.stringify({input:{mode:'normal',value:input.value},wait:true})});
    if(!response.ok) throw new Error('nested call failed');
    return {value:(await response.json()).result.value};
  }
  if(input.mode==='protocol') process.stdout.write('not-json\\n');
  if(input.mode==='flood') for(let i=0;i<140;i++) context.log('flood');
  if(input.mode==='huge') return {value:'x'.repeat(50000)};
  if(input.mode==='read'||input.mode==='read-sleep') {
    const response=await context.fetch('/api/v1/objects/records/'+input.uid+'?properties=uid,value',{method:'GET'});
    const record=await response.json();
    if(input.mode==='read-sleep') await new Promise(resolve=>setTimeout(resolve,1500));
    return {value:record.object.value};
  }
  if(input.mode==='forbidden') {
    try {await context.fetch('/api/v1/actions/edit/invoke',{method:'POST',body:'{}'});return {value:0};}
    catch {return {value:1};}
  }
  if(input.mode==='isolation') {
    const status=await readFile('/proc/self/status','utf8');
    if(process.getuid()!==65532||!status.includes('NoNewPrivs:\\t1')||!status.includes('CapEff:\\t0000000000000000')) return {value:0};
    if((await readFile('/sys/fs/cgroup/memory.max','utf8')).trim()!=='134217728'||(await readFile('/sys/fs/cgroup/pids.max','utf8')).trim()!=='64') return {value:0};
    if((await readFile('/proc/net/route','utf8')).trim().split('\\n').length!==1) return {value:0};
    if(process.env.MORIA_FUNCTION_TEST_SECRET||process.env.PGHOST||process.env.AWS_SECRET_ACCESS_KEY) return {value:0};
    try {await writeFile('/escape','unsafe');return {value:0};} catch {}
    try {await readFile('/var/run/docker.sock');return {value:0};} catch {}
    try {await fetch('http://1.1.1.1',{signal:AbortSignal.timeout(300)});return {value:0};} catch {}
    try {await fetch('http://127.0.0.1:'+input.value+'/v2/',{signal:AbortSignal.timeout(300)});return {value:0};} catch {}
    return {value:1};
  }
  return {value:input.value+1};
}
`,
    );
    await writeFile(
      join(directory, 'Dockerfile'),
      `FROM ${nodeImage}\nCOPY runtime/ /function/runtime/\nCOPY function.ts /function/\nCOPY sdk-js/ /function/sdk-js/\nCMD ["node","/function/runtime/runner.mjs","/function/function.ts"]\n`,
    );
    await docker(['build', '--network=none', '--pull=false', '--tag', repository, directory]);
    const pushed = await docker(['push', repository]);
    const digest = pushed.match(/digest: (sha256:[0-9a-f]{64})/)?.[1];
    if (!digest) throw new Error('OCI registry did not return a manifest digest');
    return { artifact: { uri: `oci://${repository}`, digest }, close };
  } catch (error) {
    await close();
    throw error;
  }
}
