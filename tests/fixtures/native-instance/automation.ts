import { publishNativeSource } from '../native-source/setup.js';

export async function prepareInstanceAutomation(input: {
  baseUrl: string;
  token: string;
  environment: string;
  dependency: { rootId: string; digest: string };
  serviceAuthority: string;
}) {
  const reference = 'instance_automation/Automation/Refresh';
  const published = await publishNativeSource(
    input.baseUrl,
    input.token,
    [
      {
        path: '.ontology.json',
        text: JSON.stringify({
          version: 1,
          name: 'Whole instance automation',
          namespace: 'instance_automation',
          dependencies: [input.dependency],
        }),
      },
      {
        path: 'refresh.resource.json',
        text: JSON.stringify({
          apiVersion: 'moria/v1alpha2',
          kind: 'Automation',
          metadata: { namespace: 'instance_automation', name: 'Refresh' },
          spec: {
            operation: 'invokeAgent',
            target: 'instance_logic/Agent/Assistant',
            application: 'source_runtime/Application/browser',
            intervalSeconds: 60,
            input: {
              mode: 'normal',
              prompt: 'native scheduled instance',
              uid: null,
              objectResource: null,
              functionResource: null,
              value: null,
            },
            overlap: 'skip',
            missed: 'coalesce',
          },
        }),
      },
    ],
    'ontology',
  );
  return {
    published,
    automation: published.resource(reference),
    profiles: [
      {
        environment: input.environment,
        projectId: published.role.projectId,
        reference,
        configuration: {
          serviceAuthority: input.serviceAuthority,
          serviceBindingRevision: 'whole-instance-auto-v1',
          targetServiceBindingRevision: 'whole-instance-agent-v1',
          resourceClass: 'automation-control',
          controlTimeoutSeconds: 120,
        },
      },
    ],
  };
}
