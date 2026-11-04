import {nativeProtectedBaseline} from '../benchmarks/native-protected-baseline.js';
import {mkdir} from 'node:fs/promises';
import {nativeProjectionCapacity} from '../benchmarks/native-projection.js';
import {assertNativeArtifacts} from '../tests/fixtures/native-common/artifact.js';
import {appendFile,readFile,readdir,writeFile} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
const rows=Number(process.env.MORIA_CAPACITY_ROWS ?? '1000000');
const output=`/work/.proof-output/population-${rows}`;
const exec=promisify(execFile);
const ignore=async<T>(fn:()=>Promise<T>)=>{try{return await fn();}catch{return undefined;}};
let sampling=false;
async function sample(){
 if(sampling)return;sampling=true;
 try{
  const processes=[];
  for(const pid of await readdir('/proc')){
   if(!/^\d+$/.test(pid))continue;
   const status=await ignore(()=>readFile(`/proc/${pid}/status`,'utf8'));
   if(!status)continue;
   const name=/^Name:\s*(.+)$/m.exec(status)?.[1];
   processes.push({pid:Number(pid),name,rssBytes:Number(/^VmRSS:\s*(\d+)/m.exec(status)?.[1]??0)*1024});
  }
  const memory:Record<string,string|undefined>={};
  for(const name of ['memory.current','memory.peak','memory.stat','memory.events','io.stat'])memory[name]=await ignore(()=>readFile(`/sys/fs/cgroup/${name}`,'utf8'));
  const roots=(await readdir('/tmp')).filter(n=>n.startsWith('catalog-storage-'));
  const disks=[];
  for(const root of roots){const value=await ignore(()=>exec('du',['-s','--block-size=1',`/tmp/${root}`]));if(value)disks.push({root,bytes:Number(value.stdout.split(/\s+/)[0])});}
  const phase=await ignore(async()=>JSON.parse(await readFile(`${output}/phase-state.json`,'utf8')));
  await appendFile(`/work/.proof-output/resources-${rows}.jsonl`,JSON.stringify({at:new Date().toISOString(),phase,processes,memory,disks})+'\n');
 }finally{sampling=false;}
}
const metadata=await assertNativeArtifacts('/work/.proof-artifacts/moria-runtime',['/work/.proof-artifacts/native_catalog']);
await mkdir(output,{recursive:true});
await writeFile(`${output}/preflight.json`,JSON.stringify(metadata,null,2));
await sample();const timer=setInterval(()=>{void sample();},15000);
try{
 const result=await nativeProjectionCapacity({rows,rowBytes:4096,output,onVerified:async fixture=>{
  await nativeProtectedBaseline(fixture,{rows,output,...(rows<1000000?{rates:[20],windowMs:2000}:{})});
 }});
 await writeFile(`${output}/result.json`,JSON.stringify(result,null,2));
 console.log(JSON.stringify(result));
}finally{clearInterval(timer);await sample();}
