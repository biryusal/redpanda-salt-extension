"""Security boundaries: administrators, transport and credential ownership."""

import json
from unittest.mock import Mock

import pytest
from salt_fixtures import configure_loader_modules  # noqa: F401


@pytest.mark.parametrize('layer', ['sasl', 'cluster', 'host_specific_override'])
@pytest.mark.parametrize('operation', ['delete', 'rotate', 'acl'])
def test_all_declared_administrators_are_protected(rp, layer, operation):
    c = rp.__pillar__['redpanda']
    c.update(kafka_enable_authorization=True, sasl={'password': 'secret'})
    if layer == 'host_specific_override':
        c[layer] = {'cluster': {'superusers': ['admin', 'breakglass']}}
    else:
        c.setdefault(layer, {})['superusers'] = ['admin', 'breakglass']
    if operation == 'acl':
        c['sasl_acls'] = [{'username': 'breakglass', 'resource_type': 'topic',
                           'resource_name': 'orders', 'operation': 'read'}]
        call = rp.acls_managed
    else:
        user = {'username': 'breakglass', 'state': 'absent'}
        if operation == 'rotate':
            user = {'username': 'breakglass', 'password': 'replacement',
                    'update_password': True}
        c['sasl_users'] = [user]
        call = rp.users_managed
    rp._api = Mock()
    rp._run = Mock()
    with pytest.raises(ValueError):
        call()
    rp._api.assert_not_called()
    rp._run.assert_not_called()


def test_sasl_http_is_refused_before_credentials_leave_host(rp):
    c = rp.__pillar__['redpanda']
    c.update(kafka_enable_authorization=True, allow_insecure_sasl=False,
             sasl={'password': 'secret'})
    rp._open = Mock()
    with pytest.raises(ValueError, match='SASL requires TLS'):
        rp._api('security/users')
    rp._open.assert_not_called()
    with pytest.raises(ValueError, match='SASL requires TLS'):
        rp.configuration()
    with pytest.raises(ValueError, match='SASL requires TLS'):
        rp.validate_security()


def test_sasl_defaults_and_broker_config_contain_no_admin_credentials(rp):
    rp.__pillar__['redpanda'].update(
        kafka_enable_authorization=True, sasl={'password': 'admin-secret'})
    result = rp.configuration()
    assert result['cluster']['admin_api_require_auth'] is True
    assert 'admin-secret' not in json.dumps(result)
    for component in ('pandaproxy', 'schema_registry'):
        listener = result['node'][component][component + '_api'][0]
        assert listener['address'] == '10.0.0.1'
        assert listener['authentication_method'] == 'http_basic'


def test_rpk_receives_credentials_only_in_environment(rp):
    rp.__pillar__['redpanda'].update(
        kafka_enable_authorization=True, sasl={'password': 'admin-secret'})
    run = Mock(return_value={'retcode': 0, 'stdout': ''})
    rp.__salt__['cmd.run_all'] = run
    ctx = rp._context()
    ctx.run(['rpk', 'cluster', 'license', 'info'])
    assert run.call_args.kwargs['env'] == {
        'RPK_USER': 'admin', 'RPK_PASS': 'admin-secret'}
    assert 'admin-secret' not in json.dumps(run.call_args.args)
    ctx.config['allow_insecure_sasl'] = False
    run.reset_mock()
    with pytest.raises(ValueError, match='SASL requires TLS'):
        ctx.run(['rpk', 'cluster', 'license', 'info'])
    run.assert_not_called()


@pytest.mark.parametrize('override', ['node', 'host_specific_override'])
def test_rpk_admin_credentials_cannot_be_reintroduced_by_overrides(rp, override):
    value = {'rpk': {'pass': 'secret'}}
    rp.__pillar__['redpanda'][override] = (
        {'node': value} if override == 'host_specific_override' else value)
    with pytest.raises(ValueError, match='Administrator credentials'):
        rp.configuration()
