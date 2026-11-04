import {nativeProjectionCapacity} from '../benchmarks/native-projection.js';
import {assertNativeArtifacts} from '../tests/fixtures/native-common/artifact.js';
import {appendFile,readFile,readdir,writeFile} from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
const output='/work/.proof-output/million';
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
  await appendFile('/work/.proof-output/resource-samples.jsonl',JSON.stringify({at:new Date().toISOString(),phase,processes,memory,disks})+'\n');
 }finally{sampling=false;}
}
const metadata=await assertNativeArtifacts('/work/.proof-artifacts/moria-runtime',['/work/.proof-artifacts/native_catalog']);
await writeFile('/work/.proof-output/preflight.json',JSON.stringify(metadata,null,2));
await sample();const timer=setInterval(()=>{void sample();},15000);
try{
 const result=await nativeProjectionCapacity({rows:1000000,rowBytes:4096,output,onVerified:async fixture=>{
  const client=await fixture.db.pool.connect();
  try{
   const build=(await client.query('SELECT id FROM moria.indexing_builds WHERE environment=$1 AND target=$2 AND status=\'active\' ORDER BY created_at DESC LIMIT 1',[fixture.environment,fixture.target])).rows[0].id;
   const count=Number((await client.query('SELECT count(*) AS n FROM moria.indexing_rows WHERE build=$1 AND partition=0',[build])).rows[0].n);
   const middle=(await client.query('SELECT uid FROM moria.indexing_rows WHERE build=$1 AND partition=0 ORDER BY uid OFFSET $2 LIMIT 1',[build,Math.floor(count/2)])).rows[0]?.uid??null;
   const scrub=(value:any):any=>Array.isArray(value)?value.map(scrub):value&&typeof value==='object'?Object.fromEntries(Object.entries(value).filter(([k])=>!['Output','Filter','Index Cond','Recheck Cond','Join Filter','Hash Cond','Merge Cond','Sort Key','Group Key','Presorted Key'].includes(k)).map(([k,v])=>[k,scrub(v)])):value;
   const plans:Record<string,unknown>={partitionRows:count,retainedUnrelatedBuild:false};
   for(const [label,after] of [['first',null],['middle',middle]])plans[`current_${label}`]=scrub((await client.query('EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) SELECT * FROM moria.indexing_reduce_page($1,0,$2)',[build,after])).rows[0]['QUERY PLAN']);
   const original=(await client.query("SELECT pg_get_functiondef('moria.indexing_reduce_page(uuid,integer,uuid)'::regprocedure) AS definition")).rows[0].definition as string;
   const old=original.replace('moria.indexing_reduce_page','pg_temp.indexing_reduce_page_legacy').replace(/r\.build\s*=\s*build_id\s+AND\s+/,'');
   if(old===original||old.includes('r.build=build_id'))throw new Error('Legacy comparison derivation failed');
   await client.query('BEGIN');
   try{
    await client.query(old);
    await client.query('DROP INDEX moria.indexing_rows_partition');
    await client.query('CREATE INDEX indexing_rows_legacy_comparison ON moria.indexing_rows(partition,uid,task,fence)');
    for(const [label,after] of [['first',null],['middle',middle]])plans[`legacy_${label}`]=scrub((await client.query('EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) SELECT * FROM pg_temp.indexing_reduce_page_legacy($1,0,$2)',[build,after])).rows[0]['QUERY PLAN']);
   }finally{await client.query('ROLLBACK');}
   await writeFile(`${output}/reducer-prefix-comparison.json`,JSON.stringify(plans,null,2));
  }finally{client.release();}
 }});
 await writeFile('/work/.proof-output/result.json',JSON.stringify(result,null,2));
 console.log(JSON.stringify(result));
}finally{clearInterval(timer);await sample();}
