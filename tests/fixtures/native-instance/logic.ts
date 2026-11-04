import type { Resource } from '../../../packages/contracts/types.js';

import { publishNativeSource } from '../native-source/setup.js';

type Artifact = { uri: string; digest: string };
type Release = { rootId: string; digest: string };

export const instanceAgentProgram = `import {StateGraph,Annotation,START,END} from '@langchain/langgraph';
export function createGraph(context) {
  const state=Annotation.Root({mode:Annotation(),prompt:Annotation(),uid:Annotation(),objectResource:Annotation(),functionResource:Annotation(),value:Annotation(),answer:Annotation()});
  return new StateGraph(state).addNode('run',async input => {
    if(input.mode==='normal')return{answer:input.prompt};
    if(input.mode==='function') {
      const response=await context.platform.fetch('/api/v1/functions/'+input.functionResource+'/invoke',{
        method:'POST',headers:{'content-type':'application/json','idempotency-key':'instance-function'},
        body:JSON.stringify({input:{value:input.value},wait:true}),
      });
      if(!response.ok)throw Error('Function tool was denied');
      let child=await response.json();
      for(let i=0;i<80&&['queued','accepted','running','preparing'].includes(child.status);i++) {
        await new Promise(resolve=>setTimeout(resolve,250));
        const current=await context.platform.fetch('/api/v1/invocations/'+child.id,{method:'GET'});
        if(!current.ok)throw Error('Function tool inspection was denied');
        child=await current.json();
      }
      if(child.status!=='succeeded')throw Error('Function tool did not succeed');
      context.log('native Function tool completed');
      return{answer:String(child.result.value)};
    }
    let prompt=input.prompt;
    if(input.mode==='read-model') {
      const response=await context.platform.fetch('/api/v1/objects/'+input.objectResource+'/'+input.uid+'?properties=name',{method:'GET'});
      if(!response.ok)throw Error('Object read was denied');
      prompt=String((await response.json()).object.name);
    }
    const result=await context.model.chat({
      messages:[{role:'user',content:'/no_think\\n'+prompt}],maxTokens:64,
    },'instance-model');
    context.log('native Model observation completed');
    return{answer:result.content};
  }).addEdge(START,'run').addEdge('run',END).compile();
}
export function result(state){return{answer:state.answer};}
`;

export const instanceEvaluatorProgram = `export default async function(input,context) {
  const actual=JSON.parse(input.actualJson),expected=JSON.parse(input.expectedJson);
  const passed=JSON.stringify(actual)===JSON.stringify(expected);
  context.log('deterministic evaluation completed');
  return{passed,score:passed?1:0,message:passed?'matched':'different'};
}
`;

export async function prepareInstanceLogic(input: {
  baseUrl: string;
  token: string;
  environment: string;
  dependencies: Release[];
  applicationReference: string;
  modelReference: string;
  artifact: Artifact;
  adapter: Artifact;
  evaluator: Artifact;
  agentSource: string;
  evaluatorSource: string;
  cases: { warehouseId: string; tableId: string; revision: string; icebergUuid: string };
  marking: string;
}) {
  const namespace = 'instance_logic';
  const reference = (kind: string, name: string) => `${namespace}/${kind}/${name}`;
  const text = { type: 'string' as const, nullable: false };
  const declarations: Resource[] = [
    {
      apiVersion: 'moria/v1alpha2',
      kind: 'AgentAdapter',
      metadata: { namespace, name: 'LangGraph' },
      spec: { artifact: input.adapter, abiVersion: 'agent/v1', capabilities: ['events', 'cancel'] },
    },
    {
      apiVersion: 'moria/v1alpha2',
      kind: 'Agent',
      metadata: { namespace, name: 'Assistant' },
      spec: {
        artifact: input.artifact,
        adapter: reference('AgentAdapter', 'LangGraph'),
        application: input.applicationReference,
        model: input.modelReference,
        inputs: {
          mode: text,
          prompt: text,
          uid: { type: 'uid', nullable: true },
          objectResource: { type: 'uid', nullable: true },
          functionResource: { type: 'uid', nullable: true },
          value: { type: 'float64', nullable: true },
        },
        outputs: { answer: text },
        limits: { timeoutSeconds: 90, memoryMiB: 256, cpuMillis: 10000 },
      },
    },
    {
      apiVersion: 'moria/v1alpha2',
      kind: 'Dataset',
      metadata: { namespace, name: 'Cases' },
      spec: {
        columns: { caseId: text, inputJson: text, expectedJson: text },
        primaryKey: ['caseId'],
        writeModes: ['replace'],
      },
    },
    {
      apiVersion: 'moria/v1alpha2',
      kind: 'EvaluationSuite',
      metadata: { namespace, name: 'Check' },
      spec: {
        agent: reference('Agent', 'Assistant'),
        fixtures: reference('Dataset', 'Cases'),
        evaluator: input.evaluator,
        repeats: 1,
      },
    },
  ];
  const published = await publishNativeSource(
    input.baseUrl,
    input.token,
    [
      {
        path: '.repository.json',
        text: JSON.stringify({
          version: 1,
          name: 'Whole instance logic',
          namespace,
          runtime: 'typescript-v1',
          dependencies: input.dependencies,
        }),
      },
      { path: 'agent.mjs', text: input.agentSource },
      { path: 'evaluator.mjs', text: input.evaluatorSource },
      ...declarations
        .filter((resource) => !['Agent', 'AgentAdapter'].includes(resource.kind))
        .map((resource) => ({
          path: `${resource.metadata.name}.${resource.kind}.resource.json`,
          text: JSON.stringify(resource),
        })),
    ],
    'repository',
    declarations.filter((resource) => ['Agent', 'AgentAdapter'].includes(resource.kind)),
  );
  const { icebergUuid, ...namespaceBinding } = input.cases;
  return {
    published,
    agent: published.resource(reference('Agent', 'Assistant')),
    suite: published.resource(reference('EvaluationSuite', 'Check')),
    cases: published.resource(reference('Dataset', 'Cases')),
    profiles: [
      {
        environment: input.environment,
        projectId: published.role.projectId,
        reference: reference('Agent', 'Assistant'),
        configuration: { resourceClass: 'standard' },
      },
      {
        environment: input.environment,
        projectId: published.role.projectId,
        reference: reference('EvaluationSuite', 'Check'),
        configuration: { resourceClass: 'standard' },
      },
      {
        environment: input.environment,
        projectId: published.role.projectId,
        reference: reference('Dataset', 'Cases'),
        configuration: { namespace: namespaceBinding, icebergUuid, markings: [input.marking] },
      },
    ],
  };
}
