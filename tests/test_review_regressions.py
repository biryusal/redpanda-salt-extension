"""Regression scenarios from the Salt/upstream comparison review."""

from salt_fixtures import configure_loader_modules  # noqa: F401
import hashlib
import json
from pathlib import Path
from unittest.mock import Mock

import pytest


def accounts(rp):
    rp.__pillar__['redpanda'].update(
        kafka_enable_authorization=True,
        sasl_use_explicit_service_accounts=True,
        sasl={
            'username': 'admin',
            'password': 'admin-secret',
            'schema_registry': {'username': 'sr', 'password': 'sr-secret'},
            'pandaproxy': {'username': 'proxy', 'password': 'proxy-secret'},
        },
    )


def live(rp, **values):
    Path(rp.CONFIG_FILE).write_text(json.dumps(values))


@pytest.mark.parametrize('name', ['admin', 'proxy'])
def test_service_account_collision_rejected_before_api(rp, name):
    accounts(rp)
    rp.__pillar__['redpanda']['sasl']['schema_registry']['username'] = name
    rp._api = Mock()
    with pytest.raises(ValueError, match='distinct'):
        rp.validate()
    with pytest.raises(ValueError, match='distinct'):
        rp.service_accounts()
    rp._api.assert_not_called()


def test_service_account_cannot_be_superuser_in_final_override(rp):
    accounts(rp)
    rp.__pillar__['redpanda']['cluster'] = {'superusers': ['admin', 'sr']}
    with pytest.raises(ValueError, match='superusers'):
        rp.configuration()


def test_active_service_password_rotation_rejected_during_prepare(rp):
    accounts(rp)
    live(
        rp,
        schema_registry_client={'scram_username': 'sr', 'scram_password': 'old-secret'},
    )
    rp._api = Mock()
    with pytest.raises(ValueError, match='new username'):
        rp.configuration()
    with pytest.raises(ValueError, match='new username'):
        rp.service_accounts()
    rp._api.assert_not_called()


def test_unverified_existing_service_account_is_never_overwritten(rp):
    accounts(rp)
    rp._api = Mock(return_value=['proxy'])
    rp._run = Mock()
    with pytest.raises(ValueError, match='cannot be verified'):
        rp.service_accounts()
    rp._api.assert_called_once_with('security/users')
    rp._run.assert_not_called()
    assert not list(Path(rp.WORK).glob('*-user.sha256'))


def test_service_account_migration_creates_new_user_and_retains_old(rp):
    accounts(rp)
    live(
        rp,
        schema_registry_client={'scram_username': 'sr-old', 'scram_password': 'old'},
        pandaproxy_client={'scram_username': 'proxy', 'scram_password': 'proxy-secret'},
    )
    rp._api = Mock(side_effect=[['sr-old', 'proxy'], None])
    rp._run = Mock(return_value='[{"principal":"exists"}]')
    assert rp.service_accounts()['changes']['account_schema_registry'] == {
        'username': 'sr',
        'action': 'created',
    }
    assert [c.args[:2] for c in rp._api.call_args_list] == [
        ('security/users',),
        ('security/users', 'POST'),
    ]
    rp._api = Mock(return_value=['sr-old', 'sr', 'proxy'])
    assert not rp.service_accounts()['changed']
    rp._api.assert_called_once_with('security/users')


def test_legacy_stamp_adoption_does_not_rotate_password(rp):
    accounts(rp)
    live(
        rp,
        **{
            component
            + '_client': {
                'scram_username': account['username'],
                'scram_password': account['password'],
            }
            for component, account in rp.__pillar__['redpanda']['sasl'].items()
            if isinstance(account, dict)
        }
    )
    Path(rp.WORK, 'schema_registry-user.sha256').write_text(
        hashlib.sha256(b'sr-secret').hexdigest()
    )
    rp._api = Mock(return_value=['sr', 'proxy'])
    rp._run = Mock(return_value='[{"principal":"exists"}]')
    assert not rp.service_accounts()['changed']
    rp._api.assert_called_once_with('security/users')
    assert (
        Path(rp.WORK, 'schema_registry-user.sha256').read_text()
        == hashlib.sha256(json.dumps(['sr', 'sr-secret']).encode()).hexdigest()
    )


def test_new_service_accounts_dry_run_does_not_write(rp):
    accounts(rp)
    rp._api = Mock(return_value=[])
    rp._run = Mock(return_value='[]')
    assert rp.service_accounts(test=True)['changed']
    rp._api.assert_called_once_with('security/users')
    assert all('list' in c.args[0] for c in rp._run.call_args_list)
    assert not list(Path(rp.WORK).glob('*-user.sha256'))


def test_old_service_account_cannot_be_deleted_during_migration(rp):
    accounts(rp)
    live(
        rp, schema_registry_client={'scram_username': 'sr-old', 'scram_password': 'old'}
    )
    rp.__pillar__['redpanda']['sasl_users'] = [
        {'username': 'sr-old', 'state': 'absent'}
    ]
    rp._api = Mock()
    with pytest.raises(ValueError, match='service accounts'):
        rp.validate_security()
    with pytest.raises(ValueError, match='service accounts'):
        rp.users_managed()
    rp._api.assert_not_called()


def test_listener_ports_drive_all_clients_with_remote_overrides(rp):
    accounts(rp)
    rp.__pillar__['redpanda']['kafka_listeners'] = [
        dict(name='internal', address='0.0.0.0', port=19092)
    ]
    rp.__pillar__['redpanda']['nodes']['broker-1']['overrides'] = {
        'kafka_listeners': [dict(name='internal', address='0.0.0.0', port=29092)]
    }
    node = rp.configuration()['node']
    assert node['redpanda']['kafka_api'][0]['port'] == 29092
    assert node['rpk']['kafka_api']['brokers'] == ['10.0.0.1:29092', '10.0.0.2:19092']
    for component in ('schema_registry', 'pandaproxy'):
        assert node[component + '_client']['brokers'] == [
            dict(address='10.0.0.1', port=29092),
            dict(address='10.0.0.2', port=19092),
        ]


def test_multiple_listeners_require_unambiguous_internal_selection(rp):
    rp.__pillar__['redpanda']['kafka_listeners'] = [
        dict(name='private', address='0.0.0.0', port=19092),
        dict(name='public', address='0.0.0.0', port=9093),
    ]
    with pytest.raises(ValueError, match='internal_kafka_listener'):
        rp.configuration()
    rp.__pillar__['redpanda']['internal_kafka_listener'] = 'private'
    assert (
        rp.configuration()['node']['rpk']['kafka_api']['brokers'][0] == '10.0.0.1:19092'
    )


def test_service_credentials_cannot_be_changed_through_node_override(rp):
    accounts(rp)
    rp.__pillar__['redpanda']['node'] = {
        'schema_registry_client': {'scram_password': 'other'}
    }
    with pytest.raises(ValueError, match='through sasl'):
        rp.configuration()


def test_internal_listener_requires_private_or_wildcard_binding(rp):
    rp.__pillar__['redpanda']['kafka_listeners'] = [
        dict(name='internal', address='127.0.0.1', port=19092)
    ]
    with pytest.raises(ValueError, match='private_ip'):
        rp.configuration()


def test_cluster_operation_uses_explicit_inventory_for_client_ports(rp):
    config = dict(rp.__pillar__['redpanda'], kafka_port=19092)
    rp.__pillar__['redpanda']['kafka_port'] = 9093
    rp._api = Mock(
        side_effect=[
            rp.configuration()['cluster'],
            [{'node_id': 1, 'config_version': 1}, {'node_id': 2, 'config_version': 1}],
        ]
    )
    build = rp.core.config.build
    seen = []

    def record(*args):
        result = build(*args)
        seen.append(result['node']['rpk']['kafka_api']['brokers'])
        return result

    rp.core.config.build = record
    rp.core.config.render.build = record
    assert not rp.cluster_config(config=config)['changed']
    assert seen == [['10.0.0.1:19092', '10.0.0.2:19092']]
    assert rp.settings()['kafka_port'] == 9093


def test_cluster_retry_after_timeout_waits_for_persisted_version(rp):
    desired = rp.configuration()['cluster']
    rp._api = Mock(side_effect=[{}, {'config_version': 12}])
    rp._wait = Mock(side_effect=RuntimeError('timeout'))
    with pytest.raises(RuntimeError, match='timeout'):
        rp.cluster_config()
    journal = Path(rp.WORK, 'cluster-pending.json')
    assert json.loads(journal.read_text()) == {'config_version': 12}
    assert not Path(rp.WORK, 'cluster-hashes.json').exists()
    rp._api = Mock(
        side_effect=[
            desired,
            [
                {'node_id': 1, 'config_version': 11},
                {'node_id': 2, 'config_version': 11},
            ],
            [
                {'node_id': 1, 'config_version': 12},
                {'node_id': 2, 'config_version': 11},
            ],
            [
                {'node_id': 1, 'config_version': 12},
                {'node_id': 2, 'config_version': 12},
            ],
        ]
    )
    outcomes = []

    def wait(predicate, description):
        outcomes.extend(predicate() for _ in range(3))
        assert outcomes == [False, False, True]

    rp._wait = wait
    assert not rp.cluster_config()['changed']
    assert not journal.exists()
    assert all(len(c.args) == 1 for c in rp._api.call_args_list)


@pytest.mark.parametrize(
    'status',
    [
        [
            {'node_id': 1, 'config_version': 3, 'invalid': ['bad']},
            {'node_id': 2, 'config_version': 3},
        ],
        [
            {'node_id': 1, 'config_version': 3, 'unknown': ['bad']},
            {'node_id': 2, 'config_version': 3},
        ],
        [{'node_id': 1, 'config_version': 3}, {'node_id': 1, 'config_version': 3}],
        {'error': 'unexpected'},
    ],
)
def test_noop_cluster_config_still_rejects_invalid_status(rp, status):
    rp._api = Mock(side_effect=[rp.configuration()['cluster'], status])
    with pytest.raises(ValueError, match='[Ii]nvalid|unknown'):
        rp.cluster_config()
    assert not Path(rp.WORK, 'cluster-hashes.json').exists()
