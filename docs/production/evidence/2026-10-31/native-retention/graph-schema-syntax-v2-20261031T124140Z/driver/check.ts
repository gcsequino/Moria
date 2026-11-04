import {database} from '/work/tests/fixtures/catalog-storage/database.js';
import {datasetGraphSchema} from './dataset-graph-schema.js';
const db=await database();try{await db.pool.query(datasetGraphSchema);console.log(JSON.stringify({event:'graph_schema_applied',classification:'isolated synthetic schema syntax proof; not full v3 qualification'}));}finally{await db.close();}
