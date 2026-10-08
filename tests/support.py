import importlib.util
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parents[1]


def load(path):
    spec = importlib.util.spec_from_file_location('module_under_test', ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def broker_module(tmp_path, inject=True):
    """Load the actual Salt adapter and isolated Python core; keep old test mocks usable."""
    from types import ModuleType
    import functools
    import sys
    import uuid

    module = load('salt/_modules/redpanda.py')
    package_name = 'redpanda_test_' + uuid.uuid4().hex
    spec = importlib.util.spec_from_file_location(
        package_name,
        ROOT / 'salt/_utils/redpanda/__init__.py',
        submodule_search_locations=[str(ROOT / 'salt/_utils/redpanda')],
    )
    package = importlib.util.module_from_spec(spec)
    sys.modules[package_name] = package
    spec.loader.exec_module(package)
    core = package.library()
    module.core = core
    import inspect

    dispatch = {}
    for area in (
        'config',
        'admin',
        'journal',
        'lifecycle',
        'security',
        'cluster',
        'storage',
    ):
        for name, function in vars(getattr(core, area)).items():
            if inspect.isfunction(function) and list(
                inspect.signature(function).parameters
            )[:1] == ['ctx']:
                # Patch the implementation module, so sibling calls see the mock.
                dispatch[name] = (function.__module__, name)
    aliases = {
        '_marker': 'pending_path',
        '_write_marker': 'write_pending',
        '_write_json': 'write_json',
        '_file_changed': 'file_changed',
        '_live_config': 'live_config',
        '_live_admin_addresses': 'live_addresses',
        '_admin_ports': 'ports',
        '_wait_cluster_config': 'wait_config',
        '_validate_service_accounts': 'validate_service_accounts',
        '_validate_users': 'validate_users',
        '_validate_acls': 'validate_acls',
        '_acl_flags': 'acl_flags',
        '_acl_matches': 'acl_matches',
    }
    dispatch.update({alias: dispatch[target] for alias, target in aliases.items()})
    io_names = {'_api': 'api', '_run': 'run', '_wait': 'wait', '_open': 'open_request'}
    original_context = module._context
    io_overrides = {}

    def context(config=None):
        module.__salt__.setdefault(
            'cp.get_file_str',
            lambda _: (ROOT / 'salt/redpanda/defaults.json').read_text(),
        )
        module.__salt__.setdefault('redpanda_lock.check', lambda token: True)
        ctx = original_context(config)
        ctx.config['paths'].update(
            work=module.WORK,
            config=module.CONFIG_FILE,
            bootstrap_env=module.BOOTSTRAP_ENV,
        )
        for name, value in io_overrides.items():
            setattr(ctx, name, value)
        return ctx

    class TestAdapter(ModuleType):
        def __getattr__(self, name):
            if name == '_merge':
                return core.config.merge
            if name == '_base_settings':
                return lambda config=None: context(config).inventory
            if name in io_names:
                return getattr(context(), io_names[name])
            if name in dispatch:
                owner, function = dispatch[name]
                return functools.partial(
                    getattr(sys.modules[owner], function), context()
                )
            raise AttributeError(name)

        def __setattr__(self, name, value):
            if name in io_names:
                io_overrides[io_names[name]] = value
            elif name in dispatch and callable(value):
                owner, function = dispatch[name]
                replacement = lambda ctx, *a, **kw: value(*a, **kw)
                setattr(
                    sys.modules[owner], function, replacement
                )
                for area in ('config', 'admin', 'journal', 'lifecycle', 'security', 'cluster', 'storage'):
                    public = getattr(core, area)
                    if hasattr(public, function):
                        setattr(public, function, replacement)
            super().__setattr__(name, value)

    module.__class__ = TestAdapter
    import time

    module.time = time
    module._context = context
    module.WORK = str(tmp_path)
    module.CONFIG_FILE = str(tmp_path / 'live-redpanda.yaml')
    module.BOOTSTRAP_ENV = str(tmp_path / 'bootstrap-superuser.conf')

    def begin(test=False):
        result = module.transaction_prepare(test=test)
        if result['needed'] and not test:
            for name in [
                'pending_restore',
                'health_before_drain',
                'maintenance_enable',
                'maintenance_wait',
                'safety_after_drain',
                'service_mask',
            ]:
                getattr(module, name)()
        return result

    def finish(test=False):
        if not module._marker().exists():
            return module.broker_start(test=test)
        if test:
            return {'changed': True}
        p = __import__('json').loads(module._marker().read_text())
        p.setdefault('steps', ['masked'])
        module._write_marker(p)
        # Native SLS actions are covered separately through Salt state dispatch.
        module.__salt__.setdefault('service.masked', lambda *a, **kw: False)
        module._run(['systemctl', 'unmask', '--runtime', 'redpanda.service'])
        module._run(['systemctl', 'daemon-reload'])
        if not module.__salt__['service.start']('redpanda-tuner'):
            raise RuntimeError('Failed to start redpanda-tuner')
        for name in ['safety_before_restart', 'broker_start', 'readiness_wait']:
            getattr(module, name)()
        for name in [
            'maintenance_disable',
            'health_after_start',
            'transaction_complete',
        ]:
            getattr(module, name)()
        return {'changed': True}

    module.begin = begin
    module.finish = finish
    if inject:
        for name, value in broker_dunders(core).items():
            setattr(module, name, value)
    return module


def broker_dunders(core=None):
    if core is None:
        core = load_core()
    return {
        '__grains__': {'id': 'broker-1', 'os_family': 'Debian'},
        '__pillar__': {
            'redpanda': {
                'allow_insecure_sasl': True,  # Mock I/O on an isolated test network.
                'version': '25.3.1',
                'nodes': {
                    'broker-1': {'private_ip': '10.0.0.1'},
                    'broker-2': {'private_ip': '10.0.0.2'},
                },
            }
        },
        '__salt__': {
            'service.status': lambda _: True,
            'service.masked': lambda *a, **kw: False,
            'redpanda_lock.check': lambda token: True,
            'cp.get_file_str': lambda _: (
                ROOT / 'salt/redpanda/defaults.json'
            ).read_text(),
        },
        '__utils__': {'redpanda.library': lambda: core},
        '__opts__': {'test': False},
    }


def load_core():
    import sys

    name = 'redpanda_test_shared'
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(
            name,
            ROOT / 'salt/_utils/redpanda/__init__.py',
            submodule_search_locations=[str(ROOT / 'salt/_utils/redpanda')],
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name].library()
