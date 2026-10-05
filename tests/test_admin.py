"""Behavior checks for admin."""

from salt_fixtures import configure_loader_modules  # noqa: F401
import json
from pathlib import Path
from unittest.mock import Mock
import unittest


def test_admin_api_preserves_put_on_trusted_leader_redirect(rp):
    import urllib.error
    import urllib.request

    redirect = urllib.error.HTTPError(
        'http://10.0.0.1:9644/v1/cluster_config',
        307,
        'redirect',
        {'Location': 'http://10.0.0.2:9644/v1/cluster_config'},
        None,
    )
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.read.return_value = b'{"config_version":3}'
    from unittest.mock import patch

    with patch.object(rp, '_open', side_effect=[redirect, response]) as request:
        assert (
            rp._api('cluster_config', 'PUT', {'upsert': {}, 'remove': []})[
                'config_version'
            ]
            == 3
        )
        req = request.call_args_list[1].args[0]
        assert req.method == 'PUT'
        assert json.loads(req.data)['remove'] == []


def test_admin_api_refuses_redirect_of_credentials_outside_cluster(rp):
    import urllib.error
    import urllib.request

    redirect = urllib.error.HTTPError(
        'http://10.0.0.1:9644',
        307,
        'redirect',
        {'Location': 'http://outside.example:9644/v1/cluster_config'},
        None,
    )
    from unittest.mock import patch

    with patch.object(rp, '_open', side_effect=redirect):
        with unittest.TestCase().assertRaisesRegex(
            RuntimeError, 'outside configured cluster'
        ):
            rp._api('cluster_config', 'PUT', {})


def test_pending_single_node_operation_does_not_need_admin_api_for_plan(rp):
    rp.__pillar__['redpanda']['nodes'] = {'broker-1': {'private_ip': '10.0.0.1'}}
    rp._write_marker({'initialized': True, 'node_id': 1})
    rp._api = Mock(side_effect=RuntimeError('unreachable'))
    assert rp.plan()['needed']
    rp._api.assert_not_called()


def test_api_uses_old_port_while_admin_port_is_changing(rp):
    import urllib.error
    from unittest.mock import patch

    Path(rp.CONFIG_FILE).write_text(
        json.dumps(
            {'rpk': {'admin_api': {'addresses': ['10.0.0.1:9644', '10.0.0.2:9644']}}}
        )
    )
    rp.__pillar__['redpanda']['admin_port'] = 9744
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.read.return_value = b'{"status":"ready"}'
    with patch.object(rp, '_open', return_value=response) as request:
        assert rp._api('status/ready', local=True)['status'] == 'ready'
        assert (
            request.call_args.args[0].full_url == 'http://10.0.0.1:9644/v1/status/ready'
        )


def test_old_ports_retained_for_peers_after_local_config_written(rp):
    rp.__pillar__['redpanda']['admin_port'] = 9744
    rp._write_marker(
        {
            'initialized': True,
            'node_id': 1,
            'admin_addresses': ['10.0.0.1:9644', '10.0.0.2:9644'],
        }
    )
    Path(rp.CONFIG_FILE).write_text(
        json.dumps({'rpk': {'admin_api': {'addresses': ['10.0.0.1:9744']}}})
    )
    assert rp._admin_ports() == [9644, 9744]
