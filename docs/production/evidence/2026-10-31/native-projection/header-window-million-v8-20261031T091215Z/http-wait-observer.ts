import fs from 'node:fs';
import pg from '/work/node_modules/pg/lib/index.js';
import {setTimeout as delay} from 'node:timers/promises';
const emit=(value:unknown)=>process.stdout.write(JSON.stringify(value)+'\n');
let selected:{pid:string,dsn:string}|undefined;
for(let attempt=0;attempt<3600&&!selected;attempt++){
 for(const pid of fs.readdirSync('/proc').filter(v=>/^\d+$/.test(v)).slice(0,512)){
  try{if(fs.readFileSync('/proc/'+pid+'/comm','utf8').trim()!=='moria-runtime')continue;
   const args=fs.readFileSync('/proc/'+pid+'/cmdline','utf8').split('\0');if(!args.includes('serve'))continue;
   const env=fs.readFileSync('/proc/'+pid+'/environ','utf8').split('\0');const dsn=env.find(value=>value.startsWith('CAPACITY_DATABASE='))?.slice('CAPACITY_DATABASE='.length);if(dsn){selected={pid,dsn};break;}
  }catch{}
 }
 if(!selected)await delay(1000);
}
if(!selected)throw new Error('HTTP_PROCESS_NOT_FOUND');
const client=new pg.Client({connectionString:selected.dsn,options:'-c default_transaction_read_only=on -c statement_timeout=1000'});
try{
 await client.connect();
 emit({event:'observer_start',at:new Date().toISOString(),intervalMillis:1000,maximumSnapshots:128,maximumActivityRows:32,maximumWaitLocks:64,readOnly:true,extensions:(await client.query("SELECT extname FROM pg_extension WHERE extname='pg_stat_statements'")).rows});
 for(let sample=0;sample<128;sample++){
  if(!fs.existsSync('/proc/'+selected.pid))break;
  const started=performance.now();
  const activity=(await client.query("SELECT pid,state,wait_event_type,wait_event,extract(epoch FROM clock_timestamp()-query_start)::text AS query_seconds,pg_blocking_pids(pid) AS blockers,left(query,400) AS query FROM pg_stat_activity WHERE usename=current_user AND pid<>pg_backend_pid() ORDER BY pid LIMIT 32")).rows;
  const blocked=activity.filter(row=>row.blockers.length>0).map(row=>row.pid);
  const locks=blocked.length?(await client.query('SELECT pid,locktype,mode,granted,relation::regclass::text AS relation,classid::text,objid::text,objsubid FROM pg_locks WHERE pid=ANY($1::int[]) AND NOT granted ORDER BY pid,locktype LIMIT 64',[blocked])).rows:[];
  emit({event:'wait_sample',at:new Date().toISOString(),sample,observerMillis:performance.now()-started,activity,locks});
  await delay(1000);
 }
 emit({event:'observer_end',at:new Date().toISOString()});
}catch(error){emit({event:'observer_end',at:new Date().toISOString(),reason:'fixture_or_http_process_closed',code:(error as {code?:string}).code??'OBSERVER_STOP'});}finally{await client.end().catch(()=>{});}
