import { runProcess } from '../../apps/local/src/runtime.js';
const runtime = await runProcess('combined');
if (!runtime) throw new Error('Native launcher did not start');
console.log(JSON.stringify({ event: 'native-local-ready' }));
