"""Behavior checks for configuration."""

from salt_fixtures import configure_loader_modules  # noqa: F401
import json
from pathlib import Path
from unittest.mock import Mock
import unittest


def test_configuration_typed_and_override_precedence(rp):
    rp.__pillar__['redpanda'].update(
        node={'redpanda': {'developer_mode': True}},
        host_specific_override={'node': {'redpanda': {'developer_mode': False}}},
    )
    c = rp.configuration()
    assert c['node']['redpanda']['developer_mode'] is False
    assert c['node']['redpanda']['empty_seed_starts_cluster'] is False
    assert c['cluster']['enable_rack_awareness'] is False
    assert len(c['node']['redpanda']['seed_servers']) == 2


def test_tls_sasl_keep_builtin_service_clients(rp):
    rp.__pillar__['redpanda'].update(
        enable_tls=True,
        tls={
            'ca_source': 'salt://ca',
            'cert_source': 'salt://cert',
            'key_source': 'salt://key',
        },
        kafka_enable_authorization=True,
        sasl={'username': 'admin', 'password': 'secret'},
    )
    c = rp.configuration()
    assert c['node']['redpanda']['kafka_api'][0]['authentication_method'] == 'sasl'
    assert c['node']['redpanda']['kafka_api_tls'][0]['name'] == 'internal'
    assert 'scram_password' not in c['node']['schema_registry_client']
    assert c['cluster']['superusers'] == ['admin']


def test_tls_service_clients_without_sasl(rp):
    rp.__pillar__['redpanda'].update(
        enable_tls=True,
        tls={
            'ca_source': 'salt://ca',
            'cert_source': 'salt://cert',
            'key_source': 'salt://key',
        },
    )
    node = rp.configuration()['node']
    for component in ('schema_registry', 'pandaproxy'):
        client = node[component + '_client']
        assert client['brokers'] == [
            {'address': '10.0.0.1', 'port': 9092},
            {'address': '10.0.0.2', 'port': 9092},
        ]
        assert client['broker_tls'] == {
            'enabled': True,
            'truststore_file': '/etc/redpanda/certs/truststore.pem',
        }
        assert 'scram_password' not in client


def test_mtls_service_clients_with_and_without_sasl(rp):
    for sasl in (False, True):
        rp.__pillar__['redpanda'].update(
            enable_tls=True,
            tls={
                'ca_source': 'salt://ca',
                'cert_source': 'salt://cert',
                'key_source': 'salt://key',
                'require_client_auth': True,
            },
            kafka_enable_authorization=sasl,
            sasl={'password': 'secret'},
        )
        node = rp.configuration()['node']
        for component in ('schema_registry', 'pandaproxy'):
            assert node[component + '_client']['broker_tls'] == {
                'enabled': True,
                'truststore_file': '/etc/redpanda/certs/truststore.pem',
                'cert_file': '/etc/redpanda/certs/node.crt',
                'key_file': '/etc/redpanda/certs/node.key',
            }


def test_package_split_and_pin(rp):
    rp.__pillar__['redpanda']['version'] = '24.1.5'
    assert rp.packages() == ['redpanda']
    rp.__salt__['pkg.version'] = lambda *args: '24.1.5'
    assert not rp.package_changes()
    rp.__pillar__['redpanda']['version'] = '24.2.1'
    assert len(rp.packages()) == 3


def test_latest_present_does_not_implicitly_upgrade(rp):
    rp.__pillar__['redpanda']['version'] = 'latest'
    rp.__salt__['pkg.version'] = lambda *args: dict.fromkeys(args, '25.3.1')
    assert not rp.package_changes()


def test_destructive_legacy_option_rejected(rp):
    rp.__pillar__['redpanda']['sasl_force_clear_data_directory'] = True
    with unittest.TestCase().assertRaisesRegex(ValueError, 'deletion'):
        rp.validate()


def test_bootstrap_env_rejects_newline_in_password(rp):
    rp.__pillar__['redpanda'].update(
        kafka_enable_authorization=True, sasl={'password': 'secret\nINJECT=1'}
    )
    with unittest.TestCase().assertRaisesRegex(ValueError, 'unsupported characters'):
        rp.validate()


def test_license_checks_remote_status_even_when_stamp_exists(rp):
    import hashlib

    rp.__pillar__['redpanda']['license'] = 'license-text'
    digest = hashlib.sha256(b'license-text').hexdigest()
    Path(rp.WORK + '/license-stamp.json').write_text(
        json.dumps({'desired': digest, 'actual': 'old'})
    )
    rp._api = Mock(
        side_effect=[
            {'loaded': True, 'license': {'sha256': 'externally-changed'}},
            {'loaded': True, 'license': {'sha256': 'new'}},
        ]
    )
    rp._run = Mock()
    assert rp.license_present()['changed']
    args = rp._run.call_args.args[0]
    assert args[:4] == ['rpk', 'cluster', 'license', 'set']
    assert not Path(args[5]).exists()
    assert 'license-text' not in args


def test_unchanged_remote_license_never_uploads(rp):
    import hashlib

    rp.__pillar__['redpanda']['license'] = 'license-text'
    digest = hashlib.sha256(b'license-text').hexdigest()
    rp._api = Mock(return_value={'loaded': True, 'license': {'sha256': digest}})
    assert not rp.license_present()['changed']
    assert rp._api.call_count == 1


def test_bootstrap_environment_quotes_systemd_values(rp):
    rp.__pillar__['redpanda'].update(
        sasl={'username': 'admin', 'password': 'p a"ss\\word$'}
    )
    assert rp.bootstrap_environment() == 'RP_BOOTSTRAP_USER="admin:p a\\"ss\\\\word$:SCRAM-SHA-256"'


def test_admin_mechanism_reaches_bootstrap_and_rpk(rp):
    for mechanism in ('SCRAM-SHA-256', 'SCRAM-SHA-512'):
        rp.__pillar__['redpanda'].update(
            kafka_enable_authorization=True,
            sasl={'username': 'admin', 'password': 'secret', 'mechanism': mechanism},
        )
        assert rp.bootstrap_environment() == 'RP_BOOTSTRAP_USER="admin:secret:' + mechanism + '"'
        assert rp.configuration()['node']['rpk']['sasl']['mechanism'] == mechanism


def test_admin_invalid_mechanism_is_rejected(rp):
    rp.__pillar__['redpanda'].update(
        kafka_enable_authorization=True,
        sasl={'username': 'admin', 'password': 'secret', 'mechanism': 'PLAIN'},
    )
    with unittest.TestCase().assertRaisesRegex(ValueError, 'SASL mechanism'):
        rp.configuration()


def test_booting_status_is_not_ready(rp):
    rp._api = Mock(return_value={'status': 'booting'})
    assert not rp.local_ready()
    rp._api.return_value = {'status': 'ready'}
    assert rp.local_ready()


def test_package_source_version_mismatch_rejected(rp):
    rp.__salt__ = {'pkg.version': lambda *args: dict.fromkeys(args, 'wrong-version')}
    with unittest.TestCase().assertRaisesRegex(ValueError, 'requested version'):
        rp.validate_packages()


def test_ipv6_rpk_endpoints_are_bracketed(rp):
    rp.__pillar__['redpanda']['nodes']['broker-1']['private_ip'] = '2001:db8::1'
    c = rp.configuration()
    assert c['node']['rpk']['admin_api']['addresses'][0] == '[2001:db8::1]:9644'
    assert c['node']['rpk']['kafka_api']['brokers'][0] == '[2001:db8::1]:9092'


def test_initialized_data_path_change_requires_migration(rp):
    old = Path(rp.WORK) / 'old-data'
    (old / 'redpanda/controller').mkdir(parents=True)
    Path(rp.CONFIG_FILE).write_text(
        json.dumps({'redpanda': {'data_directory': str(old)}})
    )
    with unittest.TestCase().assertRaisesRegex(ValueError, 'separate data migration'):
        rp.configuration()


def test_invalid_port_rejected_before_provisioning(rp):
    rp.__pillar__['redpanda']['admin_port'] = '9644'
    with unittest.TestCase().assertRaisesRegex(ValueError, 'integer port'):
        rp.validate()


def test_duplicate_broker_addresses_rejected(rp):
    rp.__pillar__['redpanda']['nodes']['broker-2']['private_ip'] = '10.0.0.1'
    with unittest.TestCase().assertRaisesRegex(ValueError, 'must be unique'):
        rp.configuration()


def test_admin_superuser_cannot_be_removed_by_cluster_override(rp):
    rp.__pillar__['redpanda'].update(
        kafka_enable_authorization=True,
        sasl={'password': 'secret'},
        cluster={'admin_api_require_auth': True, 'superusers': ['someone-else']},
    )
    with unittest.TestCase().assertRaisesRegex(ValueError, 'managing user'):
        rp.configuration()


def test_systemd_account_name_rejects_unit_injection(rp):
    rp.__pillar__['redpanda']['user'] = 'redpanda\nExecStart=/bin/false'
    with unittest.TestCase().assertRaisesRegex(ValueError, 'system account name'):
        rp.configuration()


def test_users_state_forwards_explicit_config_and_dry_run(rp):
    from support import load

    state = load('salt/_states/redpanda_broker.py')
    state.__opts__ = {'test': True}
    operation = Mock(
        return_value={
            'changed': True,
            'users': [{'username': 'app', 'action': 'created'}],
        }
    )
    state.__salt__ = {'redpanda.users_managed': operation}
    explicit = {'sasl_users': [{'username': 'app', 'password': 'secret'}]}
    result = state.users_managed('users', config=explicit)
    assert result['result'] is None and result['changes'] == {}
    operation.assert_called_once_with(test=True, config=explicit)
