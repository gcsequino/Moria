from pathlib import Path
import json,subprocess,shutil,sys
root=Path('/tmp/moria-proof-snapshots/20261031T124421293005Z-native-protected-plan');driver=Path('/tmp/moria-proof-drivers/20261031T124948Z-protected-capacity-v10');evidence=Path('/tmp/moria-reconcile-20261029/docs/production/evidence/2026-10-31/native-projection/qualified-seek-million-v10-20261031T124948Z');source=Path('/tmp/moria-reconcile-20261029')
rows=int(sys.argv[1]);assert rows in [1000,1000000]
cmd=['flock','--exclusive' if rows==1000000 else '--shared','/tmp/moria-native-proof-resources.lock','docker','run','--name',f'moria-data-qualified-seek-{rows}-v10','--memory','12g','--memory-swap','24g','--cpus','4','--network','none','--mount',f'type=bind,src={root},dst=/work','--mount',f'type=bind,src={source}/node_modules,dst=/work/node_modules,readonly','--mount',f'type=bind,src={driver},dst=/proof-driver,readonly','--mount',f'type=bind,src={source}/node_modules,dst=/node_modules,readonly','-w','/work','-e','MORIA_RUNTIME_BINARY=/work/.data-capacity-artifacts-v10/moria-runtime','-e','MORIA_CATALOG_TEST_BINARY=/work/.data-capacity-artifacts-v10/native_catalog','-e',f'MORIA_CAPACITY_ROWS={rows}','moria-test:recovery','/app/node_modules/.bin/tsx','/proof-driver/million-runner.ts']
metadata=json.loads((evidence/'manifest.json').read_text());metadata.setdefault('commands',[]).append({'population':rows,'command':cmd});(evidence/'manifest.json').write_text(json.dumps(metadata,indent=2)+'\n')
with (evidence/f'population-{rows}.txt').open('xb') as output:result=subprocess.run(cmd,stdout=output,stderr=subprocess.STDOUT)
for name in [f'population-{rows}',f'resources-{rows}.jsonl']:
 original=root/'.proof-output/v10'/name;target=evidence/name
 if original.is_dir():shutil.copytree(original,target)
 elif original.exists():shutil.copyfile(original,target)
metadata=json.loads((evidence/'manifest.json').read_text());metadata.setdefault('results',{})[str(rows)]={'exitCode':result.returncode};(evidence/'manifest.json').write_text(json.dumps(metadata,indent=2)+'\n');print(json.dumps({'population':rows,'exitCode':result.returncode,'evidence':str(evidence)}),flush=True)
raise SystemExit(result.returncode)
