"""Real Salt loader, renderer and highstate compiler; no state execution."""
import pytest
from functional_helpers import make_loaders
import salt.state


SCENARIOS = [(family, old, security, 'core') for family in ('Debian', 'RedHat') for old in (False, True) for security in (False, True)] + [('Debian', False, False, lane) for lane in ('storage', 'airgap', 'nightly', 'orchestration', 'users', 'orch_users')] + [('RedHat', False, False, 'fips')]


@pytest.mark.parametrize('family,old,security,lane', SCENARIOS)
def test_native_loader_and_highstate(tmp_path, family, old, security, lane, request):
    data = tmp_path / 'data'
    if old:
        (data / 'redpanda/controller').mkdir(parents=True)
    pillar = {'version': '25.3.1', 'data_directory': str(data), 'nodes': {'broker-1': {'private_ip': '10.0.0.1'}, 'broker-2': {'private_ip': '10.0.0.2'}}}
    if security:
        pillar.update(enable_tls=True, tls={'ca_source': 'salt://private/ca', 'cert_source': 'salt://private/cert', 'key_source': 'salt://private/key'}, kafka_enable_authorization=True, sasl={'password': 'secret'})
    if lane == 'storage':
        pillar.update(data_directory=str(tmp_path / 'mount/data'), storage={'devices': ['/dev/disk/by-id/data1', '/dev/disk/by-id/data2'], 'mountpoint': str(tmp_path / 'mount'), 'uuid': '6cde2bd7-8592-413b-940d-289c95784e62', 'format': True})
    if lane == 'airgap':
        pillar.update(airgap=True, airgap_sources=[{name: 'salt://offline/' + name + '.deb'} for name in ('redpanda', 'redpanda-rpk', 'redpanda-tuner')])
    if lane == 'nightly':
        pillar.update(development_build=True, nightly_repository='deb [signed-by=/usr/share/keyrings/nightly.gpg] https://example.test/nightly stable main', nightly_key_url='https://example.test/key')
    if lane == 'fips':
        pillar['enable_fips'] = True
    loaders = make_loaders(tmp_path, pillar, family=family, test=True)
    request.addfinalizer(loaders.reset_state)
    modules = loaders.modules
    assert 'redpanda.configuration' in modules
    assert modules['redpanda.initialized']() == old
    states = loaders.states
    assert 'redpanda_broker.transaction' in states
    with salt.state.HighState(loaders.opts) as highstate:
        sls = {'orchestration': 'redpanda.orch.deploy', 'users': 'redpanda.users', 'orch_users': 'redpanda.orch.users'}.get(lane, 'redpanda')
        high, errors = highstate.render_highstate({'base': [sls]})
        assert not errors, errors
        assert high
        assert not highstate.state.verify_high(high)
        chunks, errors = highstate.state.compile_high_data(high)
        assert not errors, errors
        ids = [chunk['__id__'] for chunk in chunks]
        if lane == 'orchestration':
            assert ids.index('redpanda-cluster') < ids.index('redpanda-apply-1') < ids.index('redpanda-apply-2')
        elif lane == 'users':
            assert set(ids) == {'redpanda-security-validate', 'redpanda-security-work-directory', 'redpanda-users', 'redpanda-acls'}
            assert not any(c['state'] in ('pkg', 'service', 'mount', 'raid', 'sysctl') for c in chunks)
        elif lane == 'orch_users':
            assert ids.index('redpanda-security-preflight') < ids.index('redpanda-security-apply')
            apply = next(c for c in chunks if c['__id__'] == 'redpanda-security-apply')
            assert apply['sls'] == 'redpanda.users'
        else:
            assert ids.index('redpanda-maintenance') < ids.index('redpanda-packages') < ids.index('redpanda-running')
            assert ('redpanda-bootstrap-config' in ids) == (not old)
            enable = next(chunk for chunk in chunks if chunk['__id__'] == 'redpanda-enable')
            assert (enable['state'], enable['fun']) == ('service', 'enabled')
            package = next(chunk for chunk in chunks if chunk['__id__'] == 'redpanda-packages')
            assert package['onlyif'] == 'test -f /var/lib/redpanda-salt/pending.json'
            unmask = next(c for c in chunks if c['__id__'] == 'redpanda-unmask')
            assert (unmask['state'], unmask['fun'], unmask['runtime']) == ('service', 'unmasked', True)
            for state_id, function in [('redpanda-systemd-reload', 'service.systemctl_reload'), ('redpanda-tuner', 'service.start')]:
                native = next(c for c in chunks if c['__id__'] == state_id)
                assert (native['state'], native['fun'], native['name']) == ('module', 'run', function)
                assert native['onlyif'] == package['onlyif']
            if lane == 'storage':
                assert ids.index('redpanda-storage-devices') < ids.index('redpanda-raid') < ids.index('redpanda-xfs') < ids.index('redpanda-data-mount')
