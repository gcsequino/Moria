from pathlib import Path
from datetime import datetime, timezone
import json, os, shutil, signal, subprocess, sys, tempfile, uuid

descriptor = Path(sys.argv[1])
d = json.loads(descriptor.read_text())
root = Path(d['snapshot']['root'])
proof = 'moria-serving-whole-' + str(uuid.uuid4())
name = 'whole-instance-backend-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
out = Path('/tmp/moria-serving-browser-evidence') / name
out.mkdir(parents=True)
temporary = Path(tempfile.mkdtemp(prefix='moria-instance-', dir='/tmp'))
temporary.chmod(0o1777)
pool_root = Path('/tmp') / ('moria-engine-scratch-' + str(uuid.uuid4()))
pool = None
code = 1
command = []
image = 'moria-test:offline-v26'
image_id = subprocess.check_output(['docker','image','inspect',image,'--format','{{.Id}}'],text=True).strip()
def interrupted(signum, frame):
    raise SystemExit(128+signum)
for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
    signal.signal(number, interrupted)
try:
    provisioner = root / 'deploy/test/provision-engine-scratch.py'
    pool = json.loads(subprocess.check_output([sys.executable,str(provisioner),str(pool_root)],text=True))
    assert pool['complete'] and Path(pool['root']) == pool_root
    dependencies = sum([['--mount',item] for item in d['snapshot']['dependencyMounts']],[])
    binaries = []
    for key,target in [('moria-runtime','moria-runtime'),('moria-indexing','moria-indexing'),('moria-native-catalog-test','moria-catalog-tests')]:
        binaries += ['--mount',f"type=bind,src={d['artifacts'][key]['path']},dst=/usr/local/bin/{target},readonly"]
    command = [
        'flock','-s','/tmp/moria-native-proof-resources.lock',
        'docker','run','--rm','--name',proof,'--network','host',
        '--group-add',str(Path('/var/run/docker.sock').stat().st_gid),'--group-add','6',
        '--label','moria.test.proof='+proof,
        '--env','pnpm_config_verify_deps_before_run=false',
        '--env','MORIA_RUNTIME_BINARY=/usr/local/bin/moria-runtime',
        '--env','MORIA_INDEXING_BINARY=/usr/local/bin/moria-indexing',
        '--env','MORIA_CATALOG_TEST_BINARY=/usr/local/bin/moria-catalog-tests',
        '--env','MORIA_NATIVE_WHOLE_INSTANCE=1',
        '--env','MORIA_NATIVE_WHOLE_INSTANCE_LOGIC=1',
        '--env','MORIA_NATIVE_WHOLE_INSTANCE_SEARCH=1',
        '--env','MORIA_NATIVE_WHOLE_INSTANCE_PIPELINE=1',
        '--env','MORIA_NATIVE_WHOLE_INSTANCE_AUTOMATION=1',
        '--env','MORIA_NATIVE_WHOLE_INSTANCE_PREFLIGHT_ONLY=1',
        '--env','MORIA_NATIVE_WHOLE_INSTANCE_MODEL_ENDPOINT=http://127.0.0.1:11436/v1/',
        '--env','MORIA_NATIVE_BROWSER_PROOF_FILE='+str(temporary/'private-state.json'),
        '--env','MORIA_TEST_CONTROLLER='+proof,'--env','MORIA_TEST_PROOF_ID='+proof,
        '--env','TMPDIR='+str(temporary),
        '--env','MORIA_NATIVE_INSTANCE_SCRATCH_POOL_JSON='+json.dumps(pool),
        '--mount',f'type=bind,src={root},dst=/work,readonly',
        '--mount',f'type=bind,src={temporary},dst={temporary}',
        '--mount',f'type=bind,src={pool_root},dst={pool_root}',
        '--mount','type=bind,src=/var/run/docker.sock,dst=/var/run/docker.sock',
        *dependencies,*binaries,
    ]
    for slot in pool['slots']:
        command += ['--device',slot['device']+':'+slot['device']+':r']
    command += ['-w','/work',image,'/app/node_modules/.bin/tsx','--test','--test-reporter=tap','--test-concurrency=1','tests/native-source-runtime.integration.test.ts']
    with (out/'integration.txt').open('xb') as log:
        code = subprocess.run(command,stdout=log,stderr=subprocess.STDOUT).returncode
finally:
    subprocess.run(['docker','stop','--time','15',proof],capture_output=True,timeout=30)
    absent = subprocess.run(['docker','inspect',proof],capture_output=True,timeout=10)
    safe = absent.returncode != 0 and b'No such object' in absent.stderr
    cleanup = []
    if safe:
        for container in subprocess.check_output(['docker','ps','-aq','--filter','label=moria.test.proof='+proof],text=True).split():
            info=json.loads(subprocess.check_output(['docker','inspect',container],text=True))[0]
            if info['Config'].get('Labels',{}).get('moria.test.proof') == proof:
                subprocess.run(['docker','rm','--force',container],check=True,capture_output=True)
        if pool is not None:
            try:
                for index,slot in enumerate(pool['slots']):
                    device = slot['device']
                    mount = pool_root / ('slot-'+str(index))
                    backing = pool_root / ('slot-'+str(index)+'.ext4')
                    assert Path(slot['registration']['mountRoot']) == mount and mount.resolve() == mount
                    actual=json.loads(subprocess.check_output(['losetup','--json','--output','NAME,BACK-FILE',device],text=True))['loopdevices']
                    assert len(actual)==1 and actual[0]['back-file']==str(backing)
                for index,slot in enumerate(pool['slots']):
                    subprocess.run(['umount',str(pool_root/('slot-'+str(index)))],check=True)
                    subprocess.run(['losetup','--detach',slot['device']],check=True)
                assert pool_root.parent == Path('/tmp') and pool_root.name.startswith('moria-engine-scratch-') and pool_root.resolve()==pool_root
                shutil.rmtree(pool_root)
            except Exception as error:
                cleanup.append({'poolRetained':str(pool_root),'reason':type(error).__name__})
                code = 1
        elif pool_root.exists():
            cleanup.append({'partialPoolRetained':str(pool_root)})
            code = 1
        assert temporary.parent==Path('/tmp') and temporary.name.startswith('moria-instance-') and temporary.resolve()==temporary
        if not cleanup: shutil.rmtree(temporary)
    else:
        cleanup.append({'controllerAbsenceUnconfirmed':proof})
        code=1
    (out/'metadata.json').write_text(json.dumps({
        'classification':'same-frozen-source all-enabled-domain backend preflight; browser tickets not issued',
        'artifact':d,'testImage':{'tag':image,'id':image_id},
        'command':command,'exit':code,'cleanup':cleanup,
    },indent=2)+'\n')
    shutil.copyfile(root/'proof-source-manifest.json',out/'source-manifest.json')
    shutil.copyfile(__file__,out/'driver.py')
    final=Path('/tmp/moria-reconcile-20261029/docs/production/evidence/2026-10-31/native-instance')/name
    shutil.copytree(out,final)
    print(json.dumps({'exit':code,'evidence':str(final)}))
    if (out/'integration.txt').exists():
        print('\n'.join((out/'integration.txt').read_text().splitlines()[-65:]))
raise SystemExit(code)
