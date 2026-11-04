import assert from 'node:assert/strict';
import {PutObjectCommand,HeadObjectCommand,DeleteObjectCommand} from '@aws-sdk/client-s3';
import {startS3} from '../tests/fixtures/storage-s3/server.js';
const s3=await startS3();
s3.client.middlewareStack.add(next=>async args=>{const request=args.request as {method?:string,headers?:Record<string,string>};if(request.method==='DELETE')console.log(JSON.stringify({method:request.method,conditionalHeaderPresent:request.headers?.['if-match']=== '"wrong-etag"'}));return next(args);},{step:'build',name:'conditionalHeaderProbe'});
try {
 const params={Bucket:s3.bucket,Key:'conditional-delete-probe'};
 const put=await s3.client.send(new PutObjectCommand({...params,Body:'retained bytes'}));
 let status:number|undefined;
 try{await s3.client.send(new DeleteObjectCommand({...params,IfMatch:'"wrong-etag"'}));}catch(error){status=(error as {$metadata?:{httpStatusCode?:number}}).$metadata?.httpStatusCode;}
 let exists=true;try{await s3.client.send(new HeadObjectCommand(params));}catch{exists=false;}
 console.log(JSON.stringify({wrongVersionStatus:status??204,objectStillExists:exists}));
 assert.equal(status,412,'Provider must enforce conditional delete');
 assert.equal((await s3.client.send(new HeadObjectCommand(params))).ContentLength,14);
 await s3.client.send(new DeleteObjectCommand({...params,IfMatch:put.ETag}));
 console.log(JSON.stringify({wrongVersion:status,correctVersion:'removed'}));
}finally{await s3.close();}
