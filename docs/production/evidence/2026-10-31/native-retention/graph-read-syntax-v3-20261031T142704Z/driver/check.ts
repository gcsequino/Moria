import assert from 'node:assert/strict';import {randomUUID} from 'node:crypto';
import {database} from '/work/tests/fixtures/catalog-storage/database.js';import {datasetGraphSchema} from './dataset-graph-schema.js';
const db=await database();try{await db.pool.query(datasetGraphSchema);
const claims={kind:'execution',session:'missing',execution:randomUUID(),task:randomUUID(),owner:randomUUID(),fence:'1',incarnation:randomUUID(),warehouse:'absent',table:'absent',revision:'v1',basis:'input'};
await assert.rejects(db.pool.query('SELECT moria.runtime_dataset_graph_read($1,$2)',[claims,[]]),(e:any)=>e.message==='UNAUTHENTICATED'||e.message==='SESSION_INVALID'||e.message==='EXECUTION_UNKNOWN');
await assert.rejects(db.pool.query('SELECT moria.runtime_dataset_graph_known($1,$2)',[claims,{}]),(e:any)=>e.message==='UNAUTHENTICATED'||e.message==='SESSION_INVALID'||e.message==='EXECUTION_UNKNOWN');
console.log(JSON.stringify({event:'graph_read_schema_applied',unissuedReadDenied:true,unissuedReuseDenied:true,classification:'isolated synthetic schema/admission proof; not v3 publication qualification'}));}finally{await db.close();}
