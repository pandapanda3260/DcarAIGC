"""Sealed selector and maintenance failure restore the predecessor service."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import plistlib
import subprocess
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('repair_coordinator_test', ROOT/'scripts/run_capture_repair.py')
coordinator = importlib.util.module_from_spec(spec); spec.loader.exec_module(coordinator)


class RepairBootstrapTest(unittest.TestCase):
    def test_environment_comes_from_plist_without_derived_identity(self):
        plist = {'EnvironmentVariables': {'DCAR_PROJECT_ROOT':'/installed', 'PATH':'/installed/bin'}}
        with patch.dict(os.environ, {'DCAR_LOADED_BUILD_ID':'injected','TIKHUB_API_KEY':'secret','DCAR_V8_DB':'/wrong'}):
            env = coordinator.bootstrap_environment(plist, Path('/private/plan.json'), 'a'*64)
        self.assertNotIn('DCAR_LOADED_BUILD_ID', env); self.assertNotIn('TIKHUB_API_KEY', env)
        self.assertNotIn('DCAR_V8_DB', env); self.assertEqual(env['DCAR_PROJECT_ROOT'], '/installed')
        self.assertEqual(env['DCAR_WRITER_ENTRY'], 'repair')
        with self.assertRaises(RuntimeError):
            coordinator.bootstrap_environment({'EnvironmentVariables':{'DCAR_LOADED_BUILD_ID':'wrong'}}, Path('/p'), 'a'*64)
        shell = (ROOT/'deploy/macos/run_writer_worker.sh').read_text()
        self.assertLess(shell.index('export DCAR_LOADED_BUILD_ID='), shell.index('"$python_bin" -m v8.capture_repair'))
        self.assertLess(shell.index('--verify-source'), shell.index('"$python_bin" -m v8.capture_repair'))
        self.assertIn('"$python_bin" -m uvicorn v8.api:app', shell)

    def test_failed_cli_restores_plist_unfreezes_and_bootstraps_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); project = root/'project'; (project/'runtime').mkdir(parents=True)
            source = root/'source'; source.mkdir()
            plist_path = root/'writer.plist'
            payload = {'Label':coordinator.LABEL,'WorkingDirectory':str(project),
                'ProgramArguments':[str(source/'deploy/macos/run_writer_worker.sh')],
                'EnvironmentVariables':{'DCAR_PROJECT_ROOT':str(project),'DCAR_WRITER_SOURCE_ROOT':str(source),
                    'DCAR_WRITER_LOCK':str(root/'writer.lock')}}
            original = plistlib.dumps(payload); plist_path.write_bytes(original)
            plan = root/'plan.json'; plan.write_text(json.dumps({'publish_operations':[], 'repair_run_id':'fixture'})); plan.chmod(0o600)
            args = SimpleNamespace(plan=plan, plan_sha256=hashlib.sha256(plan.read_bytes()).hexdigest(),
                plist=plist_path, evidence=root/'evidence', install_proposal=None)
            before = {'runtime_database_identity':{'inode':1}}
            with patch.object(coordinator,'stop_writer',return_value=before), \
                    patch.object(coordinator,'start_writer',return_value={}) as start, \
                    patch.object(coordinator,'lock_free') as lock, \
                    patch.object(coordinator,'_run_repair_process',return_value=1), \
                    patch.object(coordinator,'restore_predecessor_gates') as gates, \
                    patch.object(coordinator.subprocess,'run',return_value=SimpleNamespace(returncode=1,stderr='fixture')):
                with self.assertRaisesRegex(RuntimeError,'sealed repair failed'):
                    coordinator.execute(args)
            self.assertEqual(plist_path.read_bytes(), original)
            self.assertFalse((project/'runtime/operator-freeze.lock').exists())
            self.assertEqual(start.call_count,1); self.assertEqual(gates.call_count,1)
            self.assertTrue((args.evidence/'rollback.json').is_file()); lock.assert_called_once()

    def test_interrupted_wrapper_waits_for_its_descendant_to_finish(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); ready=root/'ready'; done=root/'done'
            child=root/'child.py'
            child.write_text("import signal,time,sys\nfrom pathlib import Path\ndef stop(*a):\n time.sleep(.2)\n Path(sys.argv[2]).write_text('settled')\n raise SystemExit(0)\nsignal.signal(signal.SIGTERM,stop)\nPath(sys.argv[1]).write_text('ready')\nwhile True:time.sleep(.1)\n")
            parent=root/'parent.py'
            parent.write_text("import subprocess,sys\np=subprocess.Popen(sys.argv[1:])\np.wait()\n")
            actual = coordinator.subprocess.Popen
            class InterruptOnce:
                def __init__(self, *a, **kw):
                    self.process=actual(*a,**kw); self.pid=self.process.pid
                def wait(self):
                    deadline=time.monotonic()+5
                    while not ready.exists() and time.monotonic()<deadline: time.sleep(.01)
                    if not ready.exists(): raise AssertionError('child did not start')
                    raise KeyboardInterrupt()
                def poll(self):return self.process.poll()
            with (root/'process.log').open('w') as output, patch.object(coordinator.subprocess,'Popen',InterruptOnce):
                with self.assertRaises(KeyboardInterrupt):
                    coordinator._run_repair_process([coordinator.sys.executable,str(parent),coordinator.sys.executable,str(child),str(ready),str(done)],cwd=root,env=os.environ.copy(),output=output)
            self.assertEqual(done.read_text(),'settled')


if __name__ == '__main__': unittest.main()
