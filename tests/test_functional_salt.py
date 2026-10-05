"""Exercise custom states through actual Salt dispatch, without broker/OS mutation."""
import pytest
import yaml
from functional_helpers import make_loaders
from support import ROOT


@pytest.fixture(scope='module')
def functional_loaders(tmp_path_factory):
    root = tmp_path_factory.mktemp('salt-functional')
    pillar = {'version': '25.3.1', 'rollout_token': 'functional-test', 'data_directory': str(root / 'data'), 'nodes': {'broker-1': {'private_ip': '10.0.0.1'}}}
    loaders = make_loaders(root, pillar)
    assert loaders.modules['redpanda_lock.acquire']('functional-test')
    yield loaders
    loaders.reset_state()


@pytest.mark.parametrize('function', ['licensed', 'service_accounts_present', 'users_managed', 'restored'])
def test_custom_noop_states_through_state_single(functional_loaders, function):
    ret = functional_loaders.modules.state.single('redpanda_broker.' + function, name='broker-core')
    assert ret.result is True
    assert ret.changes == {}


def test_validation_through_legacy_module_run(functional_loaders):
    # The SLS formula uses this syntax; verify dispatch actually invokes the function.
    ret = functional_loaders.modules.state.single('module.run', name='redpanda.validate')
    assert ret.result is True
    assert ret.changes['ret'] is True


def test_invalid_pillar_fails_module_state(functional_loaders):
    ret = functional_loaders.modules.state.single('module.run', name='redpanda.validate', pillar={'redpanda': {'version': ''}})
    assert ret.result is False


def test_formula_through_factory_salt_call(salt_factories, tmp_path):
    pillar_root = tmp_path / 'pillar'
    pillar_root.mkdir()
    (pillar_root / 'top.sls').write_text(yaml.safe_dump({'base': {'*': ['redpanda']}}))
    (pillar_root / 'redpanda.sls').write_text(yaml.safe_dump({'redpanda': {'version': '25.3.1', 'data_directory': str(tmp_path / 'data'), 'nodes': {'broker-1': {'private_ip': '10.0.0.1'}}}}))
    # Factory writes an isolated minion config and creates the CLI; no daemon starts.
    minion = salt_factories.salt_minion_daemon('broker-1', overrides={'file_client': 'local', 'file_roots': {'base': [str(ROOT / 'salt')]}, 'pillar_roots': {'base': [str(pillar_root)]}, 'grains': {'os_family': 'Debian', 'os': 'Debian', 'osrelease': '12', 'osmajorrelease': 12, 'kernel': 'Linux', 'cpuarch': 'x86_64'}})
    cli = minion.salt_call_cli()
    sync = cli.run('--local', 'saltutil.sync_all', _timeout=60)
    assert sync.returncode == 0, sync
    high = cli.run('--local', 'state.show_sls', 'redpanda', _timeout=60)
    assert high.returncode == 0, high
    assert isinstance(high.data, dict), high
    assert 'redpanda-running' in high.data
    low = cli.run('--local', 'state.show_low_sls', 'redpanda', _timeout=60)
    assert low.returncode == 0, low
    assert isinstance(low.data, list), low
    ids = [chunk['__id__'] for chunk in low.data]
    assert ids.index('redpanda-maintenance') < ids.index('redpanda-packages') < ids.index('redpanda-running')


def test_direct_state_without_rollout_is_rejected(functional_loaders):
    ret = functional_loaders.modules.state.single('redpanda_broker.licensed', name='broker-core', config={'rollout_token': None})
    assert ret.result is False
    assert 'rollout_token' in ret.comment
    assert ret.changes == {}


def test_state_explicit_config_through_real_dispatch(functional_loaders):
    ret = functional_loaders.modules.state.single('redpanda_broker.licensed', name='broker-core', config={'rollout_token': 'functional-test', 'license': None})
    assert ret.result is True
    assert ret.changes == {}


def test_application_user_through_real_state_dispatch(functional_loaders, monkeypatch, tmp_path):
    import io
    import json
    import urllib.request
    from unittest.mock import Mock
    # state.single builds a fresh loader; mock the shared HTTP transport instead.
    api = Mock(side_effect=[io.BytesIO(b'[]'), io.BytesIO(b'')])
    monkeypatch.setattr(urllib.request.OpenerDirector, 'open', api)
    config = {'rollout_token': 'functional-test', 'paths': {'work': str(tmp_path)},
              'version': '25.3.1', 'nodes': {'broker-1': {'private_ip': '10.0.0.1'}},
              'kafka_enable_authorization': True, 'allow_insecure_sasl': True,
              'sasl': {'password': 'admin-secret'},
              'sasl_users': [{'username': 'app', 'password': 'app-secret'}]}
    ret = functional_loaders.modules.state.single('redpanda_broker.users_managed', name='application-users', config=config)
    assert ret.result is True
    assert ret.changes == {'users': [{'username': 'app', 'action': 'created'}]}
    request = api.call_args.args[0]
    assert request.method == 'POST' and request.full_url.endswith('/v1/security/users')
    assert json.loads(request.data) == {'username': 'app', 'password': 'app-secret', 'algorithm': 'SCRAM-SHA-256'}


def test_lifecycle_callbacks_preserve_execution_loader(functional_loaders, monkeypatch, tmp_path):
    import json
    config = {'rollout_token': 'functional-test', 'paths': {'work': str(tmp_path)},
              'version': '25.3.1', 'nodes': {'broker-1': {'private_ip': '10.0.0.1'}}}
    marker = tmp_path / 'pending.json'
    marker.write_text(json.dumps({'initialized': True, 'node_id': 1, 'steps': ['prepared']}))
    commands = []
    monkeypatch.setitem(functional_loaders.modules, 'service.status', lambda _: False)
    monkeypatch.setitem(functional_loaders.modules, 'service.start', lambda _: True)
    def run(argv, **kwargs):
        commands.append(argv)
        return {'retcode': 0, 'stdout': ''}
    monkeypatch.setitem(functional_loaders.modules, 'cmd.run_all', run)
    result = functional_loaders.modules['redpanda.pending_restore'](config=config)
    assert result['changed'] is True
    assert result['changes'] == {'service': 'restored'}
    assert commands == [['systemctl', 'unmask', '--runtime', 'redpanda.service'], ['systemctl', 'daemon-reload']]
    assert json.loads(marker.read_text())['steps'] == ['prepared', 'restored']


@pytest.mark.parametrize('phase', ['transaction', 'masked', 'ready', 'completed'])
def test_lifecycle_phase_dry_run_through_real_dispatch(functional_loaders, phase):
    ret = functional_loaders.modules.state.single('redpanda_broker.' + phase, name='broker-core', test=True)
    assert ret.result is None
    assert ret.changes == {}


def test_native_unmask_is_idempotent_and_uses_runtime_mask(functional_loaders, monkeypatch):
    from unittest.mock import Mock
    monkeypatch.setitem(functional_loaders.opts, 'test', False)
    masked = {'value': True}
    def unmask(name, runtime):
        assert name == 'redpanda' and runtime is True
        masked['value'] = False
        return True
    action = Mock(side_effect=unmask)
    monkeypatch.setitem(functional_loaders.modules, 'service.masked', lambda name, runtime: masked['value'])
    monkeypatch.setitem(functional_loaders.modules, 'service.unmask', action)
    import salt.loader
    state = salt.loader.states(functional_loaders.opts, functions=functional_loaders.modules,
                               utils=functional_loaders.utils, serializers=functional_loaders.serializers)['service.unmasked']
    result = state('redpanda', runtime=True)
    assert result['changes'], result
    assert state('redpanda', runtime=True)['changes'] == {}
    action.assert_called_once_with('redpanda', True)


def test_native_reload_and_oneshot_tuner_propagate_failures(functional_loaders, monkeypatch):
    from unittest.mock import Mock
    monkeypatch.setitem(functional_loaders.opts, 'test', False)
    from salt.exceptions import CommandExecutionError
    reload = Mock(return_value=True)
    start = Mock(return_value=False)
    def service_start(name):
        return start(name=name)
    monkeypatch.setitem(functional_loaders.modules, 'service.systemctl_reload', reload)
    monkeypatch.setitem(functional_loaders.modules, 'service.start', service_start)
    import salt.loader
    states = salt.loader.states(functional_loaders.opts, functions=functional_loaders.modules,
                                utils=functional_loaders.utils, serializers=functional_loaders.serializers)
    states.pack['__low__'] = {'__id__': 'native-service-test'}
    state = states['module.run']
    result = state(name='service.systemctl_reload')
    assert result['result'] is True, result
    assert state(name='service.start', m_name='redpanda-tuner')['result'] is False
    start.assert_called_once_with(name='redpanda-tuner')
    reload.side_effect = CommandExecutionError('daemon reload failed')
    assert state(name='service.systemctl_reload')['result'] is False
