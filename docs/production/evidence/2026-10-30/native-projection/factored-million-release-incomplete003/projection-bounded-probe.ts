import {readFile,readdir,writeFile} from 'node:fs/promises';
import pg from 'pg';
let connection:string|undefined;
for(const pid of await readdir('/proc')){
 if(!/^\d+$/.test(pid))continue;
 try{
  const name=(await readFile(`/proc/${pid}/comm`,'utf8')).trim();
  if(!name.startsWith('moria-'))continue;
  const env=Object.fromEntries((await readFile(`/proc/${pid}/environ`,'utf8')).split('\0').filter(Boolean).map(s=>{const i=s.indexOf('=');return[s.slice(0,i),s.slice(i+1)];}));
  if(env.MORIA_INDEXING_DATABASE_SECRET_REF&&env[env.MORIA_INDEXING_DATABASE_SECRET_REF]){connection=env[env.MORIA_INDEXING_DATABASE_SECRET_REF];break;}
 }catch{}
}
if(!connection)throw new Error('Live native diagnostic credential unavailable');
const client=new pg.Client({connectionString:connection});await client.connect();
try{
 const p=(await client.query("SELECT id,stage,environment,resource,base_revision::text,build_after,row_count::text,definition->'budgets' AS budgets FROM moria.runtime_projections WHERE status='building' ORDER BY created_at DESC LIMIT 1")).rows[0];
 if(!p)throw new Error('No live building projection');
 await client.query('BEGIN READ ONLY');await client.query("SET LOCAL statement_timeout='120s'");
 const query=`WITH keys AS MATERIALIZED (
 SELECT uid FROM (
  (SELECT uid FROM moria.staged_source_rows WHERE stage=$1 AND uid>=COALESCE($5::uuid,'00000000-0000-0000-0000-000000000000'::uuid) AND ($5::uuid IS NULL OR uid>$5) ORDER BY uid LIMIT $6)
  UNION ALL
  (SELECT c.uid FROM moria.native_object_creations c WHERE c.environment=$2 AND c.object_type=$3 AND c.created_revision<=$4 AND c.uid>=COALESCE($5::uuid,'00000000-0000-0000-0000-000000000000'::uuid) AND ($5::uuid IS NULL OR c.uid>$5) AND NOT EXISTS(SELECT 1 FROM moria.staged_source_rows r WHERE r.stage=$1 AND r.uid=c.uid) ORDER BY c.uid LIMIT $6)
 ) candidates ORDER BY uid LIMIT $6
)
SELECT r.uid,octet_length(COALESCE(r.source_value,r.value)::text)+octet_length((r.value||COALESCE(o.overrides,'{}'))::text) AS bytes,
 CASE WHEN r.action_created THEN moria.runtime_projection_required_fields(r.source_value,$7::jsonb) AND moria.runtime_projection_required_fields(r.value||COALESCE(o.overrides,'{}'),$8::jsonb) ELSE true END AS shape_valid
 FROM keys k CROSS JOIN LATERAL(SELECT * FROM moria.runtime_object_base_stage($1,$2,$3,$4) r WHERE r.uid=k.uid OFFSET 0) r LEFT JOIN LATERAL (SELECT overrides FROM moria.overlay_versions o WHERE o.environment=$2 AND o.object_type=$3 AND o.uid=r.uid AND o.revision<=$4 ORDER BY revision DESC LIMIT 1) o ON TRUE ORDER BY k.uid`;
 const schemas=(await client.query('SELECT source_schema,object_schema FROM moria.runtime_projections WHERE id=$1',[p.id])).rows[0];
 const params=[p.stage,p.environment,p.resource,p.base_revision,p.build_after,p.budgets.batchRows,schemas.source_schema,schemas.object_schema.properties];
 const scrub=(value:any):any=>Array.isArray(value)?value.map(scrub):value&&typeof value==='object'?Object.fromEntries(Object.entries(value).filter(([k])=>!['Output','Filter','Index Cond','Recheck Cond','Join Filter','Hash Cond','Merge Cond','Sort Key','Group Key','Presorted Key'].includes(k)).map(([k,v])=>[k,scrub(v)])):value;
 const plans:Record<string,unknown>={projectionRowsCompleted:Number(p.row_count),staticCandidateSql:query,settings:(await client.query("SELECT current_setting('jit') AS jit,current_setting('plan_cache_mode') AS plan_cache_mode,pg_jit_available() AS jit_available")).rows[0]};
 for(const [label,mode] of [['custom','force_custom_plan'],['generic','force_generic_plan']]){
  await client.query(`SET LOCAL plan_cache_mode=${mode}`);
  plans[label]=scrub((await client.query({name:`projection_${label}`,text:`EXPLAIN(ANALYZE,BUFFERS,FORMAT JSON) ${query}`,values:params})).rows[0]['QUERY PLAN']);
 }
 await client.query('ROLLBACK');
 await writeFile('/work/.proof-output/million/live-projection-bounded-plan-comparison.json',JSON.stringify(plans,null,2));
 console.log(JSON.stringify({measured:true,completedRows:Number(p.row_count)}));
}catch(error){await writeFile('/work/.proof-output/million/live-projection-bounded-plan-probe-error.json',JSON.stringify({code:(error as {code?:string}).code??'DIAGNOSTIC_FAILED'}));process.exitCode=1;}finally{await client.end();}
