"""Behavior checks for cluster."""

from salt_fixtures import configure_loader_modules  # noqa: F401
import json
from pathlib import Path
from unittest.mock import Mock
import unittest


def test_noop_cluster_config_does_not_mutate(rp):
    rp._api = Mock(
        side_effect=[
            rp.configuration()['cluster'],
            [{'node_id': 1, 'config_version': 1}, {'node_id': 2, 'config_version': 1}],
        ]
    )
    assert rp.cluster_config() == {'changed': False, 'keys': []}
    assert [call.args for call in rp._api.call_args_list] == [
        ('cluster_config?include_defaults=true',),
        ('cluster_config/status',),
    ]


def test_cluster_config_dry_run_has_no_put(rp):
    rp._api = Mock(return_value={})
    assert rp.cluster_config(test=True)['changed']
    assert rp._api.call_count == 1


def test_cluster_update_waits_for_valid_status(rp):
    rp._api = Mock(
        side_effect=[
            {},
            {'config_version': 3},
            [
                {'node_id': 1, 'config_version': 3, 'invalid': [], 'unknown': []},
                {'node_id': 2, 'config_version': 3, 'invalid': [], 'unknown': []},
            ],
        ]
    )
    assert rp.cluster_config()['changed']
    args = rp._api.call_args_list[1].args
    assert args[0:2] == ('cluster_config', 'PUT')
    assert args[2]['remove'] == []


def test_cluster_config_waits_for_all_nodes_to_acknowledge(rp):
    rp._api = Mock(
        side_effect=[
            {},
            {'config_version': 4},
            [{'node_id': 1, 'config_version': 4}, {'node_id': 2, 'config_version': 3}],
            [{'node_id': 1, 'config_version': 4}, {'node_id': 2, 'config_version': 4}],
        ]
    )
    rp.time.sleep = Mock()
    assert rp.cluster_config()['changed']
    assert rp._api.call_count == 4


def test_masked_secret_is_idempotent_after_success(rp):
    import hashlib

    rp.__pillar__['redpanda']['cluster'] = {'cloud_storage_secret_key': 's3-secret'}
    current = rp.configuration()['cluster']
    current['cloud_storage_secret_key'] = '[secret]'
    digest = hashlib.sha256(
        json.dumps('s3-secret', sort_keys=True).encode()
    ).hexdigest()
    Path(rp.WORK + '/cluster-hashes.json').write_text(
        json.dumps({'cloud_storage_secret_key': digest})
    )
    rp._api = Mock(
        side_effect=[
            current,
            [{'node_id': 1, 'config_version': 1}, {'node_id': 2, 'config_version': 1}],
        ]
    )
    assert not rp.cluster_config()['changed']


def test_cluster_removed_managed_key_resets_only_owned_property(rp):
    desired = rp.configuration()['cluster']
    current = dict(desired, managed_old=7, foreign_setting=8)
    stamp = Path(rp.WORK) / 'cluster-managed.json'
    stamp.write_text(json.dumps(list(desired) + ['managed_old']))
    rp._api = Mock(
        side_effect=[
            current,
            {'config_version': 5},
            [{'node_id': 1, 'config_version': 5}, {'node_id': 2, 'config_version': 5}],
        ]
    )
    assert rp.cluster_config()['removed'] == ['managed_old']
    assert rp._api.call_args_list[1].args == (
        'cluster_config',
        'PUT',
        {'upsert': {}, 'remove': ['managed_old']},
    )
    assert json.loads(stamp.read_text()) == sorted(desired)
    rp._api = Mock(
        side_effect=[
            current,
            [{'node_id': 1, 'config_version': 5}, {'node_id': 2, 'config_version': 5}],
        ]
    )
    assert not rp.cluster_config()['changed']
    assert rp._api.call_args.args == ('cluster_config/status',)


def test_cluster_removal_dry_run_does_not_change_tracking(rp):
    desired = rp.configuration()['cluster']
    stamp = Path(rp.WORK) / 'cluster-managed.json'
    before = json.dumps(list(desired) + ['old'])
    stamp.write_text(before)
    rp._api = Mock(return_value=desired)
    assert rp.cluster_config(test=True)['removed'] == ['old']
    rp._api.assert_called_once_with('cluster_config?include_defaults=true')
    assert stamp.read_text() == before


def test_cluster_removal_failure_remains_recoverable(rp):
    stamp = Path(rp.WORK) / 'cluster-managed.json'
    stamp.write_text(json.dumps(['old']))
    rp._api = Mock(side_effect=[{}, {'config_version': 12}])
    rp._wait = Mock(side_effect=RuntimeError('timeout'))
    with unittest.TestCase().assertRaises(RuntimeError) as error:
        rp.cluster_config()
    assert error.exception.changes['cluster_config']['removed'] == ['old']
    assert 'old' in json.loads(stamp.read_text())
