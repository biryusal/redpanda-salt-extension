"""Safety boundaries: real filesystem reservations and runner orchestration."""
import fcntl
import os
from unittest.mock import Mock
import pytest
from support import load
from salt_fixtures import configure_loader_modules


def lock_module(tmp_path):
    module = load('salt/_modules/redpanda_lock.py')
    module.__opts__ = {'cachedir': str(tmp_path)}
    module.__salt__ = {'saltutil.running': lambda: []}
    return module


def runner(tmp_path):
    module = load('salt/_runners/redpanda_rollout.py')
    module.__opts__ = {'cachedir': str(tmp_path)}
    module.__salt__ = {}
    module.__context__ = {}
    return module


def test_reservation_is_persistent_across_loaders(tmp_path):
    one, two = lock_module(tmp_path), lock_module(tmp_path)
    assert one.acquire('first')
    assert two.acquire('first')
    with pytest.raises(RuntimeError, match='another rollout'):
        two.acquire('second')
    assert one.release('first')
    assert two.acquire('second')


@pytest.mark.parametrize('active_fun', ['state.apply', 'redpanda.broker_start'])
def test_missing_owner_and_active_state_cannot_unlock(tmp_path, active_fun):
    module = lock_module(tmp_path)
    with pytest.raises(ValueError, match='rollout_token'):
        module.check(None)
    with pytest.raises(RuntimeError, match='no rollout reservation'):
        module.check('x')
    module.acquire('x')
    module.__salt__['saltutil.running'] = lambda: [{'fun': active_fun}]
    with pytest.raises(RuntimeError, match='still running'):
        module.release('x')
    assert module.check('x')


def test_existing_master_lock_rejects_second_rollout(tmp_path):
    module = runner(tmp_path)
    fd = os.open(tmp_path / 'redpanda-rollout.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(RuntimeError, match='Another Redpanda rollout'):
            module.deploy(pillar={'redpanda': {'nodes': {'a': {}}}})
    finally:
        os.close(fd)


@pytest.mark.parametrize('missing', [False, True])
def test_failed_rollout_never_releases_reservations(tmp_path, missing):
    module = runner(tmp_path)
    execute = Mock(side_effect=[{'a': {}, 'b': {}}, {'a': True} if missing else {'a': True, 'b': True}])
    orchestrate = Mock(return_value={'retcode': 1, 'data': {}})
    module.__salt__ = {'salt.execute': execute, 'state.orchestrate': orchestrate}
    result = module.deploy(pillar={'redpanda': {'nodes': {'a': {}, 'b': {}}}})
    assert result['result'] is False and result['reservations'] == 'retained'
    assert module.__context__['retcode'] == 1
    assert len(result['token']) == 32
    assert not any(call.args[1] == 'redpanda_lock.release' for call in execute.call_args_list)
    if missing:
        orchestrate.assert_not_called()


def test_successful_rollout_passes_token_then_releases(tmp_path):
    module = runner(tmp_path)
    execute = Mock(side_effect=[{'a': {}}, {'a': True}, {'a': True}])
    orchestrate = Mock(return_value={'retcode': 0, 'data': {'master': {'state': {'result': True}}}})
    module.__salt__ = {'salt.execute': execute, 'state.orchestrate': orchestrate}
    result = module.deploy(pillar={'redpanda': {'nodes': {'a': {}}}})
    assert result['result'] is True and result['reservations'] == 'released'
    assert orchestrate.call_args.kwargs['pillar']['redpanda']['rollout_token'] == result['token']


def test_restart_probe_blocks_partition_risks(rp):
    rp.healthy = lambda: True
    risks = {k: [] for k in ('rf1_offline', 'unavailable', 'acks1_data_loss', 'full_acks_produce_unavailable')}
    risks['rf1_offline'] = ['kafka/single-replica/0']
    rp._api = Mock(side_effect=[{'maintenance_status': {'draining': True, 'finished': True}}, {'risks': risks}])
    with pytest.raises(RuntimeError, match='rf1_offline'):
        rp.restart_safety(1)
    assert rp._api.call_args.args == ('broker/pre_restart_probe',)
    assert rp._api.call_args.kwargs == {'local': True}


def test_restart_probe_failure_is_fail_closed(rp):
    rp.healthy = lambda: True
    rp._api = Mock(side_effect=[{'maintenance_status': {'draining': True, 'finished': True}}, RuntimeError('HTTP 404')])
    with pytest.raises(RuntimeError, match='404'):
        rp.restart_safety(1)


def test_finish_rechecks_before_restart_and_keeps_pending(rp):
    rp._write_marker({'initialized': True, 'node_id': 1})
    restart = Mock(return_value=True)
    rp.__salt__.update({'service.status': lambda _: True, 'service.start': lambda _: True, 'service.restart': restart})
    rp._run = Mock()
    rp.healthy = lambda: False
    with pytest.raises(RuntimeError, match='unhealthy'):
        rp.finish()
    restart.assert_not_called()
    assert rp._marker().exists()


def test_cluster_config_preserves_partial_changes_on_timeout(rp):
    rp.configuration = lambda: {'cluster': {'a': 7}}
    rp._api = Mock(side_effect=[{}, {'config_version': 12}])
    rp._wait = Mock(side_effect=RuntimeError('timeout'))
    with pytest.raises(RuntimeError) as error:
        rp.cluster_config()
    assert error.value.changes == {'cluster_config': {'keys': ['a'], 'config_version': 12}}


def test_execution_accepts_explicit_config_without_mutating_pillar(rp):
    rp._api = Mock(return_value={})
    assert not rp.license_present(config={'version': '25.3.1', 'nodes': {}})['changed']
    assert rp.__pillar__['redpanda']['nodes']


def test_state_preserves_partial_change_details():
    state = load('salt/_states/redpanda_broker.py')
    exc = RuntimeError('unhealthy after drain')
    exc.changes = {'maintenance': 'enabled'}
    state.__salt__ = {'redpanda.transaction_prepare': Mock(side_effect=exc)}
    state.__opts__ = {'test': False}
    result = state.transaction('broker')
    assert result['result'] is False
    assert result['changes'] == {'maintenance': 'enabled'}


def test_map_uses_resolved_extension_settings(rp):
    import jinja2
    import json
    from salt.utils.jinja import SerializerExtension
    from support import ROOT
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(ROOT / 'salt')), extensions=[SerializerExtension], undefined=jinja2.StrictUndefined)
    template = env.from_string("{% from 'redpanda/map.jinja' import rp with context %}{{ rp | tojson }}")
    rp.__pillar__['redpanda']['nodes']['broker-1']['overrides'] = {'admin_port': 1999}
    result = json.loads(template.render(salt={'redpanda.settings': rp.settings}, pillar=rp.__pillar__, grains=rp.__grains__))
    assert result['admin_port'] == 1999
    assert result['paths']['config'] == rp.CONFIG_FILE
