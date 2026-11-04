import fs from 'node:fs';
import pg from '/work/node_modules/pg/lib/index.js';
const native=[];let dsn;
for(const pid of fs.readdirSync('/proc').filter(v=>/^\d+$/.test(v)).slice(0,512)){
 try{
  const name=fs.readFileSync('/proc/'+pid+'/comm','utf8').trim();if(!['moria-runtime','moria-indexing','native_catalog','moria-native-ca'].includes(name))continue;
  const env=fs.readFileSync('/proc/'+pid+'/environ','utf8').split('\0');dsn??=env.find(value=>value.startsWith('CAPACITY_DATABASE='))?.slice('CAPACITY_DATABASE='.length);
  const stat=fs.readFileSync('/proc/'+pid+'/stat','utf8').split(' ');
  native.push({pid,name,io:fs.readFileSync('/proc/'+pid+'/io','utf8').trim().split('\n').map(line=>line.split(': ')).reduce((value,[key,n])=>({...value,[key]:n}),{}),userTicks:stat[13],systemTicks:stat[14]});
 }catch{}
}
if(!dsn)throw new Error('CAPACITY_PROCESS_NOT_FOUND');
const client=new pg.Client({connectionString:dsn,options:'-c default_transaction_read_only=on -c statement_timeout=1000'});client.on('error',()=>{});
try{
 await client.connect();
 const activity=(await client.query("SELECT pid,state,wait_event_type,wait_event,extract(epoch FROM clock_timestamp()-query_start)::text AS query_seconds,left(query,180) AS query FROM pg_stat_activity WHERE usename=current_user AND pid<>pg_backend_pid() ORDER BY pid LIMIT 16")).rows;
 const database=(await client.query('SELECT xact_commit::text,xact_rollback::text,blks_read::text,blks_hit::text,tup_returned::text,tup_fetched::text,temp_bytes::text,temp_files::text FROM pg_stat_database WHERE datname=current_database()')).rows[0];
 const verifiers=(await client.query('SELECT status,count(*)::text FROM moria.runtime_snapshot_verifications GROUP BY status ORDER BY status')).rows;
 const tasks=(await client.query('SELECT status,count(*)::text FROM moria.indexing_tasks GROUP BY status ORDER BY status')).rows;
 console.log(JSON.stringify({at:new Date().toISOString(),classification:'bounded-read-only-phase-diagnostic',native,database,activity,verifiers,tasks}));
}finally{await client.end().catch(()=>{});}
