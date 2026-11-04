import assert from 'node:assert/strict';
import {test} from 'node:test';import {randomUUID} from 'node:crypto';import pg from 'pg';
import {managedRetentionDataset} from '/work/tests/fixtures/native-retention/dataset.js';
for(const profile of ['uid','array'] as const)test('actual Source/S3 v2 bootstrap and rooted v3 scan/key lookup '+profile,{timeout:180000},async()=>{
 const fixture=await managedRetentionDataset({primaryKey:[profile==='uid'?'id':'tags'],writeModes:['replace','append','upsert','delete']});
 try{
  const rows=profile==='uid'?Array.from({length:64},(_,index)=>({id:randomUUID(),name:'row-'+index,tags:['東京',String(index)]})):[{id:randomUUID(),name:'first',tags:['東京','😀']},{id:randomUUID(),name:'second',tags:['😀','東京']},{id:randomUUID(),name:'empty',tags:[]}];
  const original=await fixture.publish(rows);const binding=await fixture.bind([],{[fixture.dataset]:original.head.revision});
  const out=await fixture.invoke({root:fixture.db.root,token:fixture.token,binding,mode:'graph-bootstrap',snapshots:[fixture.snapshot],expectedRows:rows.length,writerRows:{[fixture.dataset]:rows},s3Bindings:[fixture.datasetStorage]});
  assert.match(out,/'?graph_bootstrap_read/);const after=(await fixture.db.catalog.heads(fixture.environment,[fixture.dataset]))[0];assert.deepEqual(after,original.head);
  const result=(await fixture.db.pool.query('SELECT q.root,q.node,q.profile,p.scope,p.execution,p.task FROM moria.runtime_dataset_graph_roots q JOIN moria.runtime_dataset_graph_qualifications c ON c.node=q.node AND c.physical_fingerprint=q.physical_fingerprint AND c.incarnation=q.incarnation JOIN moria.runtime_dataset_producers p ON p.scope=c.producer WHERE q.root=moria.runtime_dataset_root_key($1)',[after.metadataLocation])).rows;
  assert.equal(result.length,1);assert.ok(result[0].execution);assert.ok(result[0].task);assert.equal(result[0].profile.readerProfile,'iceberg-v2-native-mor-bounded-v3');assert.match(result[0].scope,/^dataset\//);
  const counts=(await fixture.db.pool.query('SELECT count(*)::int n,min(live_references)::text minimum FROM moria.runtime_dataset_graph_counts')).rows[0];assert.ok(counts.n>0);assert.ok(BigInt(counts.minimum)>0n);
  const role=new pg.Pool({connectionString:fixture.nativeEnv.RETENTION_DATABASE});try{await assert.rejects(role.query('UPDATE moria.runtime_dataset_graph_counts SET live_references=live_references+1 WHERE node=$1',[result[0].node]),/GC_OBJECT_DENIED/);}finally{await role.end();}
  const inputRead=await fixture.read(after);assert.match(inputRead.out,/'?read/);
  const execution=(await fixture.db.pool.query('SELECT id FROM moria.runtime_executions WHERE binding=$1',[binding])).rows[0].id;await fixture.command(['execution','cancel'],{id:execution});
  console.log(JSON.stringify({profile,rows:rows.length,exactV3Readback:true,allKeysLocated:true,v2Readback:true,actualProducer:true,headUnchanged:true,ordinaryGraphCountMutationDenied:true}));
 }finally{await fixture.close();}
});
