"""Attached-volume discovery regressions; no Salt process or disk mutations."""
import ast
import json
from pathlib import Path
import unittest
ROOT = Path(__file__).resolve().parents[1]

class AttachedStorage(unittest.TestCase):
    def test_storage_discovery(self):
        import io
        import re
        import uuid
        from types import SimpleNamespace
        tree = ast.parse((ROOT / 'salt/_utils/redpanda/storage.py').read_text())
        tree.body = [n for n in tree.body if isinstance(n, ast.FunctionDef)]
        expected = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee'
        device = '/dev/disk/by-id/virtio-' + expected[:20]
        for scenario in ['mounted', 'fstab', 'explicit', 'blank', 'blank_existing', 'partitioned', 'foreign', 'ambiguous']:
            with self.subTest(scenario=scenario):
                def command(args, **kwargs):
                    self.assertFalse(kwargs['python_shell'])
                    if args[0] == 'blkid':
                        if scenario in ('blank', 'blank_existing'):
                            return {'retcode': 2, 'stdout': ''}
                        return {'retcode': 0, 'stdout': 'TYPE=' + ('ext4' if scenario == 'foreign' else 'xfs') + '\nUUID=' + expected + ('\nPTTYPE=gpt' if scenario == 'partitioned' else '')}
                    present = scenario == 'mounted' or (scenario == 'fstab' and '--fstab' in args)
                    return {'retcode': 0 if present else 1, 'stdout': json.dumps({'filesystems': [{'target': '/mnt/vectorized'}]}) if present else ''}
                def read(path):
                    return io.StringIO('existing broker configuration')
                namespace = {'json': json, 're': re, 'uuid': uuid, 'open': read,
                    'os': SimpleNamespace(path=SimpleNamespace(
                        exists=lambda p: p == device or scenario != 'blank',
                        realpath=lambda p: '/mnt/vectorized/data' if p == '/var/lib/redpanda/data' else p,
                        commonpath=__import__('os').path.commonpath)),
                    'yaml': SimpleNamespace(safe_load=lambda stream: {'redpanda': {'data_directory': '/var/lib/redpanda/data'}}),
                    }
                exec(compile(tree, 'storage.py', 'exec'), namespace)
                desired = {'storage': {'mountpoint': '/mnt/vectorized'}} if scenario == 'explicit' else {}
                def discover(initialize=True):
                    config = {'storage': dict(devices=[device], resolve=True, initialize=initialize, **desired.get('storage', {})), 'paths': {'config': '/etc/redpanda/redpanda.yaml'}}
                    return namespace['discover'](config, command)
                if scenario in ('foreign', 'ambiguous', 'blank_existing', 'partitioned'):
                    with self.assertRaises(ValueError):
                        discover()
                    continue
                result = discover()
                self.assertEqual(result['data_directory'], '/var/lib/redpanda/data')
                self.assertEqual(result['storage']['devices'], [device])
                self.assertEqual(result['storage']['format'], scenario == 'blank')
                if scenario == 'blank':
                    self.assertEqual(result, discover())
                    with self.assertRaisesRegex(ValueError, 'initialize'):
                        discover(initialize=False)
                else:
                    self.assertEqual(result['storage']['uuid'], expected)
                    self.assertEqual(result['storage']['mountpoint'], '/mnt/vectorized')

    def test_settings_and_state_map_use_extension_discovery(self):
        import copy
        import importlib.util
        from types import SimpleNamespace
        spec = importlib.util.spec_from_file_location('extension_module', ROOT / 'salt/_modules/redpanda.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        merge_tree = ast.parse((ROOT / 'salt/_utils/redpanda/config/layers.py').read_text())
        merge_tree.body = [node for node in merge_tree.body if isinstance(node, ast.FunctionDef) and node.name == 'merge']
        namespace = {'copy': copy}
        exec(compile(merge_tree, 'config.py', 'exec'), namespace)
        calls = []
        run_all = object()
        def discover(config, run):
            calls.append(config)
            self.assertIs(run, run_all)
            return {'data_directory': '/mnt/actual/data', 'storage': {'devices': ['/dev/disk/by-id/attached'], 'uuid': 'existing-uuid', 'format': False}}
        module.__pillar__ = {'redpanda': {'nodes': {'broker': {'overrides': {'storage': {'resolve': True, 'devices': ['/dev/disk/by-id/attached']}}}}}}
        module.__grains__ = {'id': 'broker'}
        module.__salt__ = {'cp.get_file_str': lambda _: (ROOT / 'salt/redpanda/defaults.json').read_text(), 'cmd.run_all': run_all}
        module._library = lambda: SimpleNamespace(config=SimpleNamespace(merge=namespace['merge']),
            storage=SimpleNamespace(discover=discover), new_context=lambda config, *args: SimpleNamespace(config=config))
        settings = module.settings()
        self.assertEqual(settings['data_directory'], '/mnt/actual/data')
        self.assertEqual(settings['storage']['devices'], ['/dev/disk/by-id/attached'])
        self.assertEqual(len(calls), 1)
        self.assertIn("salt['redpanda.settings']()", (ROOT / 'salt/redpanda/map.jinja').read_text())
        module.__pillar__['redpanda']['nodes']['broker']['overrides'] = {}
        module.settings()
        self.assertEqual(len(calls), 1)  # Explicit-device users never run discovery.


if __name__ == '__main__':
    unittest.main()
