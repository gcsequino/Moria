# Platform execution

Native workers execute independently from the API. A registered worker claims a fenced task for a supported domain and resource class. The scheduler owns durable firing and recovery. Inspect and cancel executions through their supported APIs or the generated clients.

- [Independent process entry points](production/EXECUTION-PROCESSES.md)
- [Execution and receipt contract](production/contracts/execution.md)
- [Native worker authority](production/contracts/job-authority.md)
- [Sandbox isolation and lifecycle](production/contracts/sandbox.md)
- [Scheduled Automation](production/contracts/automations.md)

Default management startup registers no bundled workload. Configure each required native worker explicitly for the instance. Use the [native composition guide](production/NATIVE-INSTANCE-COMPOSITION.md) for the roles and bindings used in integrated acceptance.
