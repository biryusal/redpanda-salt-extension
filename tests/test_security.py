"""Independent security operations, exact ACL filters and rollout exclusion."""

import json
from pathlib import Path
from unittest.mock import Mock
import pytest
from salt_fixtures import configure_loader_modules  # noqa: F401
from support import load


def security(rp, **kwargs):
    rp.__pillar__['redpanda'].update(
        version='',
        kafka_enable_authorization=True,
        sasl={'username': 'admin', 'password': 'admin-secret'},
        **kwargs
    )


def rule(**kwargs):
    return dict(
        dict(
            username='app',
            resource_type='topic',
            resource_name='orders',
            operation='read',
        ),
        **kwargs
    )


def row(acl):
    return dict(
        principal='User:' + acl['username'],
        host=acl.get('host', '*'),
        resource_type=acl['resource_type'].replace('_', '').upper(),
        resource_name=acl['resource_name'],
        resource_pattern_type=acl.get('pattern', 'literal').upper(),
        operation=acl['operation'].replace('_', '').upper(),
        permission=acl.get('permission', 'allow').upper(),
    )


def test_user_creation_requires_no_broker_version_or_package_operations(rp):
    security(rp, sasl_users=[dict(username='app', password='app-secret')])
    rp._api = Mock(side_effect=[[], None])
    rp._run = Mock()
    assert rp.validate_security() == {'changed': False}
    assert rp.users_managed()['changes'] == {
        'users': [{'username': 'app', 'action': 'created'}]
    }
    rp._run.assert_not_called()
    assert not rp._marker().exists()
    assert not Path(rp.CONFIG_FILE).exists()


def test_security_cannot_mutate_during_pending_deployment(rp):
    security(
        rp, sasl_users=[dict(username='app', password='secret')], sasl_acls=[rule()]
    )
    rp._write_marker({'initialized': True, 'node_id': 1})
    rp._api = Mock()
    rp._run = Mock()
    for operation in (rp.validate_security, rp.users_managed, rp.acls_managed):
        with pytest.raises(RuntimeError, match='Pending broker deployment'):
            operation()
    rp._api.assert_not_called()
    rp._run.assert_not_called()


@pytest.mark.parametrize('mechanism', ['SCRAM-SHA-256', 'SCRAM-SHA-512'])
def test_acl_create_rechecks_and_is_idempotent(rp, mechanism):
    acl = rule()
    security(rp, sasl_acls=[acl])
    rp.__pillar__['redpanda']['sasl']['mechanism'] = mechanism
    rp._run = Mock(
        side_effect=[
            json.dumps({'matches': None}),
            '',
            json.dumps({'matches': [row(acl)]}),
        ]
    )
    result = rp.acls_managed()
    assert result['changes'] == {'acls': [dict(acl, action='create')]}
    command = rp._run.call_args_list[1]
    assert command.args[0][:4] == ['rpk', 'security', 'acl', 'create']
    assert '--allow-principal' in command.args[0] and 'User:app' in command.args[0]
    assert 'admin-secret' not in command.args[0]
    assert 'sasl.mechanism=' + mechanism in command.args[0]
    assert command.kwargs['env'] == {'RPK_USER': 'admin', 'RPK_PASS': 'admin-secret'}
    assert 'admin-secret' not in json.dumps(result)
    rp._run = Mock(return_value=json.dumps({'matches': [row(acl)]}))
    assert not rp.acls_managed()['changed']
    assert rp._run.call_count == 1


def test_acl_delete_uses_every_identity_filter_and_retains_other_rules(rp):
    acl = rule(
        state='absent',
        permission='deny',
        host='10.0.0.99',
        pattern='prefixed',
        resource_name='orders-',
    )
    security(rp, sasl_acls=[acl])
    rp._run = Mock(
        side_effect=[
            json.dumps({'matches': [row(acl)]}),
            '',
            json.dumps({'matches': []}),
        ]
    )
    assert rp.acls_managed()['changed']
    command = rp._run.call_args_list[1].args[0]
    for flag, value in [
        ('--deny-principal', 'User:app'),
        ('--deny-host', '10.0.0.99'),
        ('--operation', 'read'),
        ('--topic', 'orders-'),
        ('--resource-pattern-type', 'prefixed'),
    ]:
        assert command[command.index(flag) + 1] == value
    assert '--no-confirm' in command
    assert rp._run.call_count == 3


def test_acl_dry_run_only_reads_and_writes_no_tracking(rp):
    acl = rule()
    security(rp, sasl_acls=[acl])
    rp._run = Mock(return_value='{"matches":[]}')
    before = list(Path(rp.WORK).iterdir())
    assert rp.acls_managed(test=True)['changed']
    assert rp._run.call_count == 1 and rp._run.call_args.args[0][3] == 'list'
    assert list(Path(rp.WORK).iterdir()) == before


@pytest.mark.parametrize(
    'response',
    [
        '{"filters":[{"message":"authorization failed"}],"matches":[]}',
        '{"error":"unknown"}',
        '[{"principal":"unexpected"}]',
        json.dumps({'matches': [row(rule(resource_name='orders-archive'))]}),
    ],
)
def test_acl_lookup_errors_and_broad_matches_prevent_mutation(rp, response):
    security(rp, sasl_acls=[rule(state='absent')])
    rp._run = Mock(return_value=response)
    with pytest.raises(RuntimeError):
        rp.acls_managed()
    assert rp._run.call_count == 1


def test_acl_cli_success_without_effect_is_not_reported_as_success(rp):
    security(rp, sasl_acls=[rule()])
    rp._run = Mock(side_effect=['{"matches":[]}', '', '{"matches":[]}'])
    with pytest.raises(RuntimeError, match='desired state') as error:
        rp.acls_managed()
    assert error.value.changes == {'acls': [dict(rule(), action='create')]}


@pytest.mark.parametrize(
    'acl',
    [
        rule(username='admin'),
        rule(username='*'),
        rule(username='User:app'),
        rule(resource_name='orders,other'),
        rule(pattern='any'),
        rule(operation='any'),
        rule(resource_type='cluster', resource_name='*'),
        rule(unrecognized='field'),
    ],
)
def test_invalid_acl_rejected_before_commands(rp, acl):
    security(rp, sasl_acls=[acl])
    rp._run = Mock()
    with pytest.raises(ValueError):
        rp.acls_managed()
    rp._run.assert_not_called()


def test_active_service_account_acl_is_protected(rp):
    security(rp, sasl_acls=[rule(username='old-sr', state='absent')])
    Path(rp.CONFIG_FILE).write_text(
        json.dumps({'schema_registry_client': {'scram_username': 'old-sr'}})
    )
    with pytest.raises(ValueError, match='service accounts'):
        rp.validate_security()


def test_security_runner_reserves_all_hosts_and_invokes_only_users_orchestration(
    tmp_path,
):
    runner = load('salt/_runners/redpanda_rollout.py')
    runner.__opts__ = {'cachedir': str(tmp_path)}
    runner.__context__ = {}
    execute = Mock(
        side_effect=[{'a': {}, 'b': {}}, {'a': True, 'b': True}, {'a': True, 'b': True}]
    )
    orchestrate = Mock(
        return_value={'retcode': 0, 'data': {'master': {'security': {'result': True}}}}
    )
    runner.__salt__ = {'salt.execute': execute, 'state.orchestrate': orchestrate}
    result = runner.users(pillar={'redpanda': {'nodes': {'a': {}, 'b': {}}}})
    assert result['result'] is True and result['reservations'] == 'released'
    assert orchestrate.call_args.args == ('redpanda.orch.users',)
    assert execute.call_args_list[1].args == (['a', 'b'], 'redpanda_lock.acquire')


def test_failed_security_runner_keeps_every_reservation(tmp_path):
    runner = load('salt/_runners/redpanda_rollout.py')
    runner.__opts__ = {'cachedir': str(tmp_path)}
    runner.__context__ = {}
    execute = Mock(side_effect=[{'a': {}}, {'a': True}])
    runner.__salt__ = {
        'salt.execute': execute,
        'state.orchestrate': Mock(return_value={'retcode': 1, 'data': {}}),
    }
    assert (
        runner.users(pillar={'redpanda': {'nodes': {'a': {}}}})['reservations']
        == 'retained'
    )
    assert execute.call_count == 2


from salt_fixtures import configure_loader_modules  # noqa: F401
import json
from pathlib import Path
from unittest.mock import Mock
import unittest


def test_service_accounts_acl_empty_matches_are_created(rp):
    rp.__pillar__['redpanda'].update(
        kafka_enable_authorization=True,
        sasl_use_explicit_service_accounts=True,
        sasl={
            'password': 'super-secret',
            'schema_registry': {'username': 'sr', 'password': 'sr-secret'},
            'pandaproxy': {'username': 'proxy', 'password': 'proxy-secret'},
        },
    )
    rp._api = Mock(return_value=[])
    rp._run = Mock(side_effect=lambda argv: '{"matches":[]}' if 'list' in argv else '')
    assert rp.service_accounts()['changed']
    creates = [c for c in rp._run.call_args_list if 'create' in c.args[0]]
    assert len(creates) == 4
    assert all(
        'sr-secret' not in c.args[0] and 'proxy-secret' not in c.args[0]
        for c in creates
    )


def _user_pillar(rp, users):
    rp.__pillar__['redpanda'].update(
        kafka_enable_authorization=True,
        sasl={'password': 'admin-secret'},
        sasl_users=users,
    )


def test_application_user_create_then_repeat_is_noop(rp):
    _user_pillar(
        rp,
        [
            {
                'username': 'app',
                'password': 'app-secret',
                'update_password': True,
                'mechanism': 'SCRAM-SHA-512',
            }
        ],
    )
    rp._api = Mock(side_effect=[[], None])
    result = rp.users_managed()
    assert result['changes'] == {'users': [{'username': 'app', 'action': 'created'}]}
    assert rp._api.call_args.args == (
        'security/users',
        'POST',
        {'username': 'app', 'password': 'app-secret', 'algorithm': 'SCRAM-SHA-512'},
    )
    stamp = Path(rp.WORK) / 'users-hashes.json'
    assert 'app-secret' not in stamp.read_text()
    assert stamp.stat().st_mode & 0o777 == 0o600
    rp._api = Mock(return_value=['app'])
    assert not rp.users_managed()['changed']
    rp._api.assert_called_once_with('security/users')


def test_application_user_password_rotation_is_explicit(rp):
    _user_pillar(rp, [{'username': 'app', 'password': 'new-secret'}])
    rp._api = Mock(return_value=['app'])
    assert not rp.users_managed()['changed']
    rp.__pillar__['redpanda']['sasl_users'][0]['update_password'] = True
    assert rp.users_managed()['changed']
    assert rp._api.call_args.args == (
        'security/users/app',
        'PUT',
        {'password': 'new-secret', 'algorithm': 'SCRAM-SHA-256'},
    )
    rp._api.reset_mock()
    assert not rp.users_managed()['changed']
    rp._api.assert_called_once_with('security/users')


def test_application_user_delete_and_encoded_username(rp):
    _user_pillar(rp, [{'username': 'team/app ?', 'state': 'absent'}])
    stamp = Path(rp.WORK) / 'users-hashes.json'
    stamp.write_text(json.dumps({'team/app ?': 'digest'}))
    rp._api = Mock(side_effect=[['team/app ?'], None])
    assert rp.users_managed()['changed']
    assert rp._api.call_args.args == ('security/users/team%2Fapp%20%3F', 'DELETE')
    assert json.loads(stamp.read_text()) == {}
    rp._api = Mock(return_value=[])
    assert not rp.users_managed()['changed']
    rp._api.assert_called_once_with('security/users')


def test_application_users_dry_run_has_no_mutations_or_stamps(rp):
    _user_pillar(
        rp,
        [
            {'username': 'new', 'password': 'secret'},
            {'username': 'old', 'state': 'absent'},
            {'username': 'rotate', 'password': 'secret', 'update_password': True},
        ],
    )
    rp._api = Mock(return_value=['old', 'rotate'])
    result = rp.users_managed(test=True)
    assert len(result['users']) == 3 and result['changed']
    assert 'secret' not in json.dumps(result)
    rp._api.assert_called_once_with('security/users')
    assert not (Path(rp.WORK) / 'users-hashes.json').exists()


def test_application_user_validation_precedes_any_mutation(rp):
    invalid = [
        [
            {'username': 'app', 'password': 'secret'},
            {'username': 'app', 'state': 'absent'},
        ],
        [{'username': 'app', 'password': 'secret', 'update_password': 'true'}],
        [{'username': 'app', 'password': 'secret', 'mechanism': 'PLAIN'}],
        [{'username': 'app', 'state': 'unknown'}],
        [{'username': 'app'}],
        [{'username': 'bad\nname', 'password': 'secret'}],
        [{'username': 'admin', 'state': 'absent'}],
        [{'username': 'admin', 'password': 'secret', 'update_password': True}],
        ['app'],
    ]
    for users in invalid:
        _user_pillar(rp, users)
        rp._api = Mock()
        with unittest.TestCase().assertRaises(ValueError):
            rp.users_managed()
        rp._api.assert_not_called()
    _user_pillar(rp, [{'username': 'sr', 'state': 'absent'}])
    rp.__pillar__['redpanda'].update(
        sasl_use_explicit_service_accounts=True,
        sasl={
            'password': 'secret',
            'schema_registry': {'username': 'sr'},
            'pandaproxy': {'username': 'proxy'},
        },
    )
    with unittest.TestCase().assertRaisesRegex(ValueError, 'service accounts'):
        rp.users_managed()


def test_application_users_require_authorization_and_valid_remote_list(rp):
    _user_pillar(rp, [{'username': 'app', 'password': 'secret'}])
    rp.__pillar__['redpanda']['kafka_enable_authorization'] = False
    rp._api = Mock()
    with unittest.TestCase().assertRaisesRegex(
        ValueError, 'kafka_enable_authorization'
    ):
        rp.validate_security()
    rp._api.assert_not_called()
    rp.__pillar__['redpanda']['kafka_enable_authorization'] = True
    rp._api.return_value = {'users': ['app']}
    with unittest.TestCase().assertRaisesRegex(RuntimeError, 'user-list'):
        rp.users_managed()


def test_application_user_failure_preserves_partial_changes_without_secrets(rp):
    _user_pillar(
        rp,
        [
            {'username': 'app', 'password': 'secret-a'},
            {'username': 'other', 'password': 'secret-b'},
        ],
    )
    rp._api = Mock(side_effect=[[], None, RuntimeError('HTTP 503')])
    with unittest.TestCase().assertRaises(RuntimeError) as error:
        rp.users_managed()
    assert error.exception.changes == {
        'users': [{'username': 'app', 'action': 'created'}]
    }
    assert 'secret' not in json.dumps(error.exception.changes)
    rp._api = Mock(side_effect=[['app'], None])
    assert rp.users_managed()['changes'] == {
        'users': [{'username': 'other', 'action': 'created'}]
    }
