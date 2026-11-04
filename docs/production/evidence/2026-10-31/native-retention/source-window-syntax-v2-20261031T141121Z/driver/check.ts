import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {database} from '/work/tests/fixtures/catalog-storage/database.js';
import {sourceRowsSchema} from './source-rows-schema.js';
import {datasetDeltaSchema} from './dataset-delta-schema.js';
const db=await database();try{
 await db.pool.query(sourceRowsSchema);await db.pool.query(datasetDeltaSchema);
 const role='source_dml_'+randomUUID().replaceAll('-','');
 await db.pool.query(`CREATE ROLE ${role}; GRANT USAGE ON SCHEMA moria TO ${role}; GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA moria TO ${role}`);
 const cx=await db.pool.connect();try{
  await cx.query('BEGIN');await cx.query(`SET LOCAL ROLE ${role}`);
  await assert.rejects(cx.query("INSERT INTO moria.native_source_row_versions(store,uid,from_source_version,to_source_version,raw,mapped) SELECT gen_random_uuid(),gen_random_uuid(),1,NULL,NULL,'{}'::jsonb WHERE false"),/SOURCE_STORE_AUTHORITY_REQUIRED/);await cx.query('ROLLBACK');
  await cx.query('BEGIN');await cx.query(`SET LOCAL ROLE ${role}`);
  await assert.rejects(cx.query('SELECT moria.runtime_source_store_mutation_check($1,$2,$3,1,$4,$5)',[randomUUID(),'no-environment','no-object',randomUUID(),'insert']),/permission denied/);await cx.query('ROLLBACK');
 }finally{cx.release();}
 await assert.rejects(db.pool.query('SELECT moria.runtime_source_window_open($1,$2,1,$3,$4,$5)',[randomUUID(),randomUUID(),randomUUID(),randomUUID(),'bootstrap']),/BUILD_UNKNOWN|LEASE_LOST/);
 console.log(JSON.stringify({event:'source_window_schema_applied',ordinaryDmlDenied:true,privateStateGuardDenied:true,unissuedWindowDenied:true,classification:'isolated synthetic schema and admission-negative proof; not delta publication qualification'}));
}finally{await db.close();}
