"""Validate rendered orchestration/node ordering with Salt's actual DAG."""
import jinja2
from salt.utils.jinja import SerializerExtension
import shlex
import yaml
from salt.utils.requisite import DependencyGraph
from support import ROOT


def check_graph(rp, initialized=False, security=False, orchestration=False):
    if security:
        rp.__pillar__['redpanda'].update(enable_tls=True, tls={'ca_source': 'salt://ca', 'cert_source': 'salt://cert', 'key_source': 'salt://key'}, kafka_enable_authorization=True, sasl={'password': 'secret'})
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(str(ROOT / 'salt')), undefined=jinja2.StrictUndefined, extensions=[SerializerExtension])
    env.filters['quote'] = shlex.quote
    functions = {'redpanda.settings': rp.settings, 'redpanda.bootstrap_environment': rp.bootstrap_environment, 'redpanda.configuration': rp.configuration, 'redpanda.packages': rp.packages, 'redpanda.initialized': lambda: initialized, 'redpanda.bootstrap_needed': lambda: not initialized, 'file.file_exists': lambda _: False}
    functions['slsutil.merge'] = lambda a, b, **kw: rp._merge(a, b)
    chunks = {}
    def load(sls, parents=()):
        path = sls.replace('.', '/') + '.sls'
        if not (ROOT / 'salt' / path).exists():
            path = sls.replace('.', '/') + '/init.sls'
        high = yaml.safe_load(env.get_template(path).render(salt=functions, grains=rp.__grains__, pillar=rp.__pillar__)) or {}
        for include in high.pop('include', []):
            load(include, parents + (sls,))
        for id, body in high.items():
            if id in chunks:
                chunks[id]['__sls_included_from__'] = list(set(chunks[id]['__sls_included_from__']) | set(parents))
                continue
            function, args = next(iter(body.items()))
            state, fun = function.split('.', 1)
            low = {'__id__': id, '__sls__': sls, '__sls_included_from__': list(parents), 'state': state, 'fun': fun, 'name': id}
            for arg in args:
                if isinstance(arg, dict):
                    low.update(arg)
            chunks[id] = low
    load('redpanda.orch.deploy' if orchestration else 'redpanda.redpanda_broker.apply')
    graph = DependencyGraph()
    for chunk in chunks.values():
        graph.add_chunk(chunk, False)
    for chunk in chunks.values():
        assert not graph.add_requisites(chunk, []), chunk
    ordered = list(graph.aggregate_and_order_chunks(10000))
    ids = [(entry if isinstance(entry, dict) else entry[1])['__id__'] for entry in ordered]
    if orchestration:
        assert ids.index('redpanda-apply-1') < ids.index('redpanda-apply-2')
        assert ids.index('redpanda-recover-2') < ids.index('redpanda-cluster')
    else:
        lifecycle = ['redpanda-transaction', 'redpanda-pending-restore', 'redpanda-health-before-drain', 'redpanda-maintenance', 'redpanda-drain', 'redpanda-safety-after-drain', 'redpanda-mask', 'redpanda-packages', 'redpanda-node-config', 'redpanda-unmask', 'redpanda-systemd-reload', 'redpanda-tuner', 'redpanda-safety-before-restart', 'redpanda-start', 'redpanda-readiness', 'redpanda-maintenance-clear', 'redpanda-health-after-start', 'redpanda-enable', 'redpanda-running']
        assert [ids.index(name) for name in lifecycle] == sorted(ids.index(name) for name in lifecycle)
