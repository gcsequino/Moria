import {nativeProtectedBaseline} from '/work/benchmarks/native-protected-baseline.js';
import {mkdir} from 'node:fs/promises';
import {nativeProjectionCapacity} from '/work/benchmarks/native-projection.js';
import {assertNativeArtifacts} from '/work/tests/fixtures/native-common/artifact.js';
import {appendFile,readFile,readdir,writeFile} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
const rows=Number(process.env.MORIA_CAPACITY_ROWS ?? '1000000');
const output=`/work/.proof-output/v10/population-${rows}`;
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
   const io=await ignore(()=>readFile(`/proc/${pid}/io`,'utf8'));
   const stat=await ignore(()=>readFile(`/proc/${pid}/stat`,'utf8'));
   const fields=stat?.slice(stat.lastIndexOf(')')+2).trim().split(/\s+/);
   const counters=Object.fromEntries((io??'').split('\n').flatMap(line=>{const pair=/^(rchar|wchar|syscr|syscw|read_bytes|write_bytes):\s*(\d+)$/.exec(line);return pair?[[pair[1],pair[2]]]:[];}));
   processes.push({pid:Number(pid),name,rssBytes:Number(/^VmRSS:\s*(\d+)/m.exec(status)?.[1]??0)*1024,io:counters,cpuTicks:fields?{user:fields[11],system:fields[12]}:undefined});
  }
  const memory:Record<string,string|undefined>={};
  for(const name of ['memory.current','memory.peak','memory.stat','memory.events','io.stat'])memory[name]=await ignore(()=>readFile(`/sys/fs/cgroup/${name}`,'utf8'));
  const roots=(await readdir('/tmp')).filter(n=>n.startsWith('catalog-storage-'));
  const disks=[];
  for(const root of roots){const value=await ignore(()=>exec('du',['-s','--block-size=1',`/tmp/${root}`]));if(value)disks.push({root,bytes:Number(value.stdout.split(/\s+/)[0])});}
  const phase=await ignore(async()=>JSON.parse(await readFile(`${output}/phase-state.json`,'utf8')));
  await appendFile(`/work/.proof-output/v10/resources-${rows}.jsonl`,JSON.stringify({at:new Date().toISOString(),phase,processes,memory,disks})+'\n');
 }finally{sampling=false;}
}
const metadata=await assertNativeArtifacts('/work/.data-capacity-artifacts-v10/moria-runtime',['/work/.data-capacity-artifacts-v10/native_catalog']);
await mkdir(output,{recursive:true});
await writeFile(`${output}/preflight.json`,JSON.stringify(metadata,null,2));
await sample();const timer=setInterval(()=>{void sample();},15000);
try{
 const result=await nativeProjectionCapacity({rows,rowBytes:4096,output,onVerified:async fixture=>{
  const stats=async()=>{await fixture.db.pool.query('SELECT pg_stat_clear_snapshot()');return (await fixture.db.pool.query(`SELECT xact_commit::text,xact_rollback::text,blks_read::text,blks_hit::text,tup_returned::text,tup_fetched::text,temp_files::text,temp_bytes::text,deadlocks::text FROM pg_stat_database WHERE datname=current_database()`)).rows[0];};
  await writeFile(`${output}/postgres-before-http.json`,JSON.stringify(await stats(),null,2));
  await nativeProtectedBaseline(fixture,{rows,output,...(rows<1000000?{rates:[20],windowMs:2000}:{})});
  await writeFile(`${output}/postgres-after-http.json`,JSON.stringify(await stats(),null,2));
 }});
 await writeFile(`${output}/result.json`,JSON.stringify(result,null,2));
 console.log(JSON.stringify(result));
}finally{clearInterval(timer);await sample();}
