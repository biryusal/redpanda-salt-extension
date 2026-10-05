"""Operations keep settings and partial effects separate, including nested calls."""

import json
from unittest.mock import Mock
from support import ROOT, load_core


def security_context(core, work, username, password):
    work.mkdir()
    desired = core.config.merge(
        json.loads((ROOT / 'salt/redpanda/defaults.json').read_text()),
        {
            'nodes': {'broker-1': {'private_ip': '10.0.0.1'}},
            'paths': {'work': str(work), 'config': str(work / 'redpanda.yaml')},
            'kafka_enable_authorization': True,
            'allow_insecure_sasl': True,
            'sasl': {'password': 'admin-secret'},
            'sasl_users': [{'username': username, 'password': password}],
        },
    )
    return core.new_context(desired, desired, 'broker-1', {'id': 'broker-1'}, {})


def test_nested_reconciliation_keeps_credentials_and_progress_separate(tmp_path):
    core = load_core()
    outer = security_context(core, tmp_path / 'outer', 'app-a', 'secret-a')
    inner = security_context(load_core(), tmp_path / 'inner', 'app-b', 'secret-b')
    inner.api = Mock(side_effect=[[], None])

    def request(path, method='GET', data=None):
        if method == 'GET':
            return []
        core.security.users_managed(inner)
        assert data['password'] == 'secret-a'
        return None

    outer.api = Mock(side_effect=request)
    core.security.users_managed(outer)
    assert outer.changes == {'users': [{'username': 'app-a', 'action': 'created'}]}
    assert inner.changes == {'users': [{'username': 'app-b', 'action': 'created'}]}
    assert inner.api.call_args.args[2]['password'] == 'secret-b'
    for ctx in (outer, inner):
        text = (
            tmp_path / ('outer' if ctx is outer else 'inner') / 'users-hashes.json'
        ).read_text()
        assert 'secret-' not in text
        assert 'secret-' not in json.dumps(ctx.changes)
