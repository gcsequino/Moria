const originalExit=process.exit;process.exit=((code)=>{console.error('EXIT_TRACE',code,new Error().stack);return originalExit(code)}) as typeof process.exit;process.on('SIGTERM',()=>console.error('SIGNAL_TRACE',process.pid,process.listenerCount('SIGTERM')));process.on('exit',code=>console.error('EXIT_EVENT',code,process.exitCode));
import { runProcess } from '../../apps/local/src/runtime.js';
const runtime = await runProcess('combined');
if (!runtime) throw new Error('Native launcher did not start');
console.log(JSON.stringify({ event: 'native-local-ready' }));
