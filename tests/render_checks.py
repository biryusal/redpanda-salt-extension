from pathlib import Path
import json
import jinja2
from salt.utils.jinja import SerializerExtension
import shlex
import yaml
from support import ROOT


def test_all_sls_render_and_requisites_resolve(rp, family, initialized, security):
    rp.__grains__['os_family'] = family
    if security:
        rp.__pillar__['redpanda'].update(enable_tls=True, tls={'ca_source': 'salt://ca', 'cert_source': 'salt://cert', 'key_source': 'salt://key'}, kafka_enable_authorization=True, sasl={'password': 'p:ass\"word'})
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(ROOT / 'salt')), undefined=jinja2.StrictUndefined, extensions=[SerializerExtension])
    env.filters['quote'] = shlex.quote
    salt = {'redpanda.settings': rp.settings, 'redpanda.bootstrap_environment': rp.bootstrap_environment, 'redpanda.configuration': rp.configuration, 'redpanda.packages': rp.packages, 'redpanda.initialized': lambda: initialized, 'redpanda.bootstrap_needed': lambda: not initialized, 'redpanda.recovery_needed': lambda: False, 'file.file_exists': lambda _: False}
    salt['slsutil.merge'] = lambda a, b, **kw: rp._merge(a, b)
    states = {}
    for file in (ROOT / 'salt/redpanda').rglob('*.sls'):
        content = yaml.safe_load(env.get_template(str(file.relative_to(ROOT / 'salt'))).render(salt=salt, grains=rp.__grains__, pillar=rp.__pillar__)) or {}
        assert isinstance(content, dict), file
        if 'orch' not in file.parts:
            states.update({k: v for k, v in content.items() if k != 'include'})
        elif file.name == 'deploy.sls':
            ids = list(content)
            assert ids.index('redpanda-apply-1') < ids.index('redpanda-apply-2')
            req = content['redpanda-apply-2']['salt.state']
            assert {'require': [{'salt': 'redpanda-apply-1'}]} in req
        else:
            assert content['redpanda-security-preflight']['salt.state']
            assert {'require': [{'salt': 'redpanda-security-preflight'}]} in content['redpanda-security-apply']['salt.state']
    for name, state in states.items():
        for args in state.values():
            for arg in args:
                if isinstance(arg, dict) and 'require' in arg:
                    for requisite in arg['require']:
                        kind, target = next(iter(requisite.items()))
                        if kind != 'sls':
                            assert target in states, (name, target)
    package_state = states['redpanda-packages']
    pending = rp.settings()['paths']['work'] + '/pending.json'
    assert {'onlyif': 'test -f ' + shlex.quote(pending)} in next(iter(package_state.values()))
    if rp.settings()['storage']['devices']:
        for state_id in ('redpanda-raid', 'redpanda-xfs', 'redpanda-storage-uuid'):
            if state_id in states:
                args = next(iter(states[state_id].values()))
                assert any({'module': 'redpanda-storage-devices'} in arg.get('require', []) for arg in args if isinstance(arg, dict))
    contents = next(x['contents'] for x in states['redpanda-stage-node']['file.managed'] if isinstance(x, dict) and 'contents' in x)
    assert isinstance(contents, str)
    assert json.loads(contents)['redpanda']['data_directory'] == rp.settings()['data_directory']


def check_optional_lanes(rp, lane):
    c = rp.__pillar__['redpanda']
    if lane == 'storage':
        c['data_directory'] = '/mnt/vectorized/data'
        c['storage'] = {'devices': ['/dev/disk/by-id/data-1', '/dev/disk/by-id/data-2'], 'uuid': '6cde2bd7-8592-413b-940d-289c95784e62', 'format': True}
    elif lane == 'airgap':
        c.update(airgap=True, airgap_sources=[{p: 'salt://offline/' + p + '.deb'} for p in rp.packages()])
    elif lane == 'nightly':
        c.update(development_build=True, nightly_repository='deb [signed-by=/usr/share/keyrings/nightly.gpg] https://example.test/nightly stable main', nightly_key_url='https://example.test/key')
    elif lane == 'fips':
        rp.__grains__['os_family'] = 'RedHat'
        c['enable_fips'] = True
    elif lane == 'proxy':
        c.update(create_pkg_mgr_proxy=True, https_proxy='http://proxy.example:3128')
    test_all_sls_render_and_requisites_resolve(rp, rp.__grains__['os_family'], False, False)
