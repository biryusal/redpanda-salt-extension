"""Failure and retry boundaries of individually dispatched lifecycle phases."""

import json
from pathlib import Path
from unittest.mock import Mock
import pytest
from salt_fixtures import configure_loader_modules  # noqa: F401


def pending(rp, steps):
    rp._write_marker({'initialized': True, 'node_id': 1, 'steps': steps})


def test_mask_without_successful_drain_safety_is_rejected(rp):
    pending(rp, ['prepared'])
    rp._run = Mock()
    with pytest.raises(RuntimeError, match='requires safe_to_update'):
        rp.service_mask()
    rp._run.assert_not_called()


def test_failed_readiness_cannot_clear_maintenance_or_journal(rp):
    pending(rp, ['started'])
    rp._wait = Mock(side_effect=RuntimeError('not ready'))
    rp._api = Mock()
    with pytest.raises(RuntimeError, match='not ready'):
        rp.readiness_wait()
    with pytest.raises(RuntimeError, match='requires ready'):
        rp.maintenance_disable()
    rp._api.assert_not_called()
    assert json.loads(rp._marker().read_text())['steps'] == ['started']


def test_restart_rechecks_health_despite_prior_safety_checkpoint(rp):
    pending(rp, ['safe_to_restart'])
    restart = Mock(return_value=True)
    rp.__salt__['service.restart'] = restart
    rp.healthy = lambda: False
    with pytest.raises(RuntimeError, match='unhealthy'):
        rp.broker_start()
    restart.assert_not_called()
    assert 'started' not in json.loads(rp._marker().read_text())['steps']


def test_repeating_successful_start_does_not_restart_again(rp):
    pending(rp, ['safe_to_restart', 'started'])
    restart = Mock(return_value=True)
    rp.__salt__['service.restart'] = restart
    assert rp.broker_start()['changed'] is False
    restart.assert_not_called()


def test_final_health_failure_preserves_journal_and_bootstrap_secret(rp):
    pending(rp, ['healthy_after_start'])
    Path(rp.BOOTSTRAP_ENV).write_text('secret')
    rp._wait = Mock(return_value=True)
    rp.wait_healthy = Mock(side_effect=RuntimeError('peer unavailable'))
    with pytest.raises(RuntimeError, match='peer unavailable'):
        rp.transaction_complete()
    assert rp._marker().exists()
    assert Path(rp.BOOTSTRAP_ENV).exists()


def test_retry_discards_stale_safety_and_readiness_checkpoints(rp):
    pending(rp, ['safe_to_update', 'safe_to_restart', 'started', 'healthy_after_start'])
    rp.transaction_prepare()
    assert json.loads(rp._marker().read_text())['steps'] == ['prepared']
    with pytest.raises(RuntimeError, match='requires safe_to_update'):
        rp.service_mask()


def test_failed_maintenance_request_does_not_advance_checkpoint(rp):
    pending(rp, ['healthy_before_drain'])
    rp.wait_healthy = Mock(return_value=True)
    rp._api = Mock(
        side_effect=[
            {'maintenance_status': {'draining': False}},
            RuntimeError('connection lost'),
        ]
    )
    with pytest.raises(RuntimeError, match='connection lost'):
        rp.maintenance_enable()
    assert json.loads(rp._marker().read_text())['steps'] == ['healthy_before_drain']


def test_fresh_broker_needs_start_even_when_files_and_packages_match(rp):
    rp.initialized = lambda: False
    rp._file_changed = lambda *args: False
    rp.package_changes = lambda: False
    assert rp.plan()['needed'] is True


def test_failed_native_unmask_blocks_restart(rp):
    pending(rp, ['masked'])
    rp.__salt__['service.masked'] = Mock(return_value=True)
    restart = Mock()
    rp.__salt__['service.restart'] = restart
    with pytest.raises(RuntimeError, match='still runtime masked'):
        rp.safety_before_restart()
    with pytest.raises(RuntimeError, match='requires safe_to_restart'):
        rp.broker_start()
    restart.assert_not_called()


from salt_fixtures import configure_loader_modules  # noqa: F401
import json
from pathlib import Path
from unittest.mock import Mock
import unittest


def test_unchanged_node_never_enters_maintenance_or_masks(rp):
    rp.plan = Mock(return_value={'initialized': True, 'needed': False, 'node_id': 1})
    rp._api = Mock()
    rp._run = Mock()
    assert not rp.begin()['needed']
    rp._api.assert_not_called()
    rp._run.assert_not_called()
    assert not rp._marker().exists()


def test_dry_run_never_mutates_lifecycle(rp):
    rp.plan = Mock(return_value={'initialized': True, 'needed': True, 'node_id': 1})
    rp._api = Mock()
    rp._run = Mock()
    assert rp.begin(test=True)['needed']
    rp._api.assert_not_called()
    rp._run.assert_not_called()
    assert not rp._marker().exists()


def test_restart_disabled_blocks_before_mutation(rp):
    rp.__pillar__['redpanda']['restart_node'] = False
    rp.plan = Mock(return_value={'initialized': True, 'needed': True, 'node_id': 1})
    with unittest.TestCase().assertRaisesRegex(RuntimeError, 'restart_node'):
        rp.begin()
    assert not rp._marker().exists()


def test_drain_failure_keeps_marker_without_masking(rp):
    rp.plan = Mock(return_value={'initialized': True, 'needed': True, 'node_id': 1})
    rp.wait_healthy = Mock(return_value=True)
    rp._api = Mock(
        side_effect=[
            {'status': 'ready'},
            {'maintenance_status': {'draining': False}},
            None,
            {'maintenance_status': {'errors': True}},
        ]
    )
    rp._run = Mock()
    with unittest.TestCase().assertRaisesRegex(ValueError, 'drain failed'):
        rp.begin()
    assert rp._marker().exists()
    rp._run.assert_not_called()


def test_resume_drained_operation_refuses_unhealthy_cluster(rp):
    rp._write_marker({'initialized': True, 'needed': True, 'node_id': 1})
    rp.__salt__['service.status'] = lambda _: True
    rp._api = Mock(
        side_effect=[
            {'maintenance_status': {'draining': True}},
            {'maintenance_status': {'finished': True}},
        ]
    )
    rp.healthy = Mock(return_value=False)
    rp.wait_healthy = Mock(side_effect=RuntimeError('unhealthy cluster'))
    rp._wait = Mock(return_value=True)
    rp._run = Mock()
    with unittest.TestCase().assertRaisesRegex(RuntimeError, 'unhealthy'):
        rp.begin()
    rp._run.assert_not_called()


def test_failed_restart_never_exits_maintenance(rp):
    rp._write_marker({'initialized': True, 'needed': True, 'node_id': 1})
    rp._run = Mock()
    rp._api = Mock()
    rp.__salt__ = {
        'service.status': lambda _: True,
        'service.start': lambda _: True,
        'service.restart': lambda _: False,
    }
    rp.restart_safety = Mock(return_value=True)
    with unittest.TestCase().assertRaisesRegex(RuntimeError, 'start/restart'):
        rp.finish()
    rp._api.assert_not_called()
    assert rp._marker().exists()


def test_pending_fresh_bootstrap_remains_parallel_on_retry(rp):
    rp._write_marker({'initialized': False, 'node_id': None})
    rp.initialized = lambda: True
    assert rp.bootstrap_needed()
    assert not rp.recovery_needed()


def test_successful_restart_exits_maintenance_after_readiness(rp):
    rp._write_marker({'initialized': True, 'needed': True, 'node_id': 1})
    Path(rp.BOOTSTRAP_ENV).write_text('secret')
    events = []
    rp._run = lambda args: events.append(args)
    rp.__salt__ = {
        name: lambda service, name=name: events.append([name, service]) or True
        for name in (
            'service.status',
            'service.start',
            'service.restart',
            'service.enable',
        )
    }
    rp.restart_safety = Mock(return_value=True)
    rp._api = lambda path, method='GET', **kwargs: events.append([method, path]) or (
        {'maintenance_status': {'draining': True}}
        if path == 'brokers/1'
        else {'status': 'ready'}
    )
    rp.wait_healthy = lambda: events.append(['healthy']) or True
    assert rp.finish()['changed']
    assert (
        events.index(['GET', 'status/ready'])
        < events.index(['DELETE', 'brokers/1/maintenance'])
        < events.index(['healthy'])
    )
    assert not rp._marker().exists()
    assert not Path(rp.BOOTSTRAP_ENV).exists()


def test_fresh_finish_does_not_wait_for_whole_cluster(rp):
    rp._write_marker({'initialized': False, 'needed': True, 'node_id': None})
    rp._run = Mock()
    rp._api = Mock(return_value={'status': 'ready'})
    rp.wait_healthy = Mock()
    rp.__salt__ = {
        name: lambda service: True for name in ('service.start', 'service.enable')
    }
    rp.__salt__['service.status'] = lambda _: False
    assert rp.finish()['changed']
    rp.wait_healthy.assert_not_called()
    assert not any(c.args[0].startswith('brokers/') for c in rp._api.call_args_list)


def test_stopped_existing_service_restored_before_api_planning(rp):
    rp.initialized = lambda: True
    events = []
    rp.__salt__ = {
        'service.status': lambda _: False,
        'service.start': lambda _: events.append('start') or True,
    }
    rp._api = lambda *args, **kwargs: events.append('ready') or {'status': 'ready'}
    assert rp.restore_service()['changed']
    assert events == ['start', 'ready']


def test_systemd_dropin_difference_requires_restart(rp):
    rp.initialized = lambda: True
    rp._file_changed = lambda source, dest: source.endswith('/systemd.conf')
    rp.package_changes = lambda: False
    rp._api = Mock(
        side_effect=[
            [{'internal_rpc_address': '10.0.0.1', 'node_id': 1}],
            [{'node_id': 1, 'restart': False}],
        ]
    )
    assert rp.plan()['needed']


def test_restoration_dry_run_does_not_start_service(rp):
    rp.initialized = lambda: True
    start = Mock()
    rp.__salt__ = {'service.status': lambda _: False, 'service.start': start}
    assert rp.restore_service(test=True)['changed']
    start.assert_not_called()


def test_single_pending_node_can_restart_without_healthy_peers(rp):
    rp.__salt__['service.status'] = lambda _: True
    rp.__pillar__['redpanda']['nodes'] = {'broker-1': {'private_ip': '10.0.0.1'}}
    rp._write_marker({'initialized': True, 'node_id': 1})
    rp._run = Mock()
    rp._api = Mock(side_effect=RuntimeError('unreachable'))
    rp.begin()
    rp._api.assert_not_called()
    assert rp._run.call_args.args[0][:2] == ['systemctl', 'mask']


def test_healthy_requires_exact_addresses_and_node_ids(rp):
    health = {'is_healthy': True, 'all_nodes': [1, 2]}
    brokers = [
        {'node_id': 1, 'internal_rpc_address': '10.0.0.1'},
        {'node_id': 2, 'internal_rpc_address': '10.0.0.2'},
    ]
    rp._api = Mock(side_effect=[health, brokers])
    assert rp.healthy()
    for wrong in [
        [dict(brokers[0]), dict(brokers[1], internal_rpc_address='10.0.0.99')],
        [dict(brokers[0]), dict(brokers[1], internal_rpc_address='10.0.0.1')],
        [dict(brokers[0]), dict(brokers[1], node_id=901)],
        [dict(brokers[0]), dict(brokers[1], node_id=1)],
        brokers + [{'node_id': 3, 'internal_rpc_address': '10.0.0.3'}],
        [brokers[0]],
        [None, brokers[1]],
    ]:
        rp._api = Mock(side_effect=[health, wrong])
        assert not rp.healthy()
    rp._api = Mock(side_effect=[dict(health, all_nodes=[1, 1]), brokers])
    assert not rp.healthy()
    rp._api = Mock(return_value=dict(health, nodes_down=[2]))
    assert not rp.healthy()
    rp._api.assert_called_once_with('cluster/health_overview')
