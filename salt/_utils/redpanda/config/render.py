"""Build broker and cluster configuration from resolved settings."""

import copy
from . import layers


def _address(host, port):
    return f'[{host}]:{port}' if ':' in host else f'{host}:{port}'


def _kafka_listener(c, ip):
    listeners = c.get(
        'kafka_listeners', [dict(name='internal', address=ip, port=c['kafka_port'])]
    )
    if (
        not isinstance(listeners, list)
        or not listeners
        or any(
            (
                not isinstance(x, dict)
                or not isinstance(x.get('name'), str)
                or (not x['name'])
                or (not isinstance(x.get('address'), str))
                or (not x['address'])
                or (type(x.get('port')) is not int)
                or (not 1 <= x['port'] <= 65535)
                for x in listeners
            )
        )
    ):
        raise ValueError(
            'kafka_listeners requires named listeners with addresses and integer ports'
        )
    if len({x['name'] for x in listeners}) != len(listeners):
        raise ValueError('Kafka listener names must be unique')
    name = c.get('internal_kafka_listener')
    if name is None:
        name = (
            'internal'
            if any((x['name'] == 'internal' for x in listeners))
            else listeners[0]['name'] if len(listeners) == 1 else None
        )
    matches = [x for x in listeners if x['name'] == name]
    if len(matches) != 1:
        raise ValueError('Set internal_kafka_listener to one configured Kafka listener')
    if matches[0]['address'] not in (ip, '0.0.0.0', '::'):
        raise ValueError(
            'Internal Kafka listener must bind the broker private_ip or a wildcard address'
        )
    return matches[0]


def build(c, minion_id, inventory_config=None):
    me = c['nodes'][minion_id]
    ip = me['private_ip']
    advertised = me.get('advertised_ip', ip)
    nodes = list(c['nodes'].values())
    _kafka_listener(c, ip)
    kafka_endpoints = []
    for n in nodes:
        remote = layers.merge(inventory_config or c, n.get('overrides', {}))
        listener = _kafka_listener(remote, n['private_ip'])
        kafka_endpoints.append(dict(address=n['private_ip'], port=listener['port']))
    listeners = copy.deepcopy(
        c.get(
            'kafka_listeners', [dict(name='internal', address=ip, port=c['kafka_port'])]
        )
    )
    if c['kafka_enable_authorization']:
        for listener in listeners:
            listener.setdefault('authentication_method', 'sasl')
    advertised_listeners = c.get(
        'advertised_kafka_listeners', [dict(x, address=advertised) for x in listeners]
    )
    broker = dict(
        empty_seed_starts_cluster=False,
        data_directory=c['data_directory'],
        rpc_server=dict(address=ip, port=c['rpc_port']),
        advertised_rpc_api=dict(address=ip, port=c['rpc_port']),
        kafka_api=listeners,
        advertised_kafka_api=advertised_listeners,
        admin=[dict(address=ip, port=c['admin_port'])],
        seed_servers=[
            dict(host=dict(address=n['private_ip'], port=c['rpc_port'])) for n in nodes
        ],
    )
    cluster = dict(
        rpc_server_tcp_recv_buf=65536,
        enable_rack_awareness=bool(me.get('rack')),
        kafka_enable_authorization=c['kafka_enable_authorization'],
    )
    if me.get('rack'):
        broker['rack'] = me['rack']
    rpk = dict(
        kafka_api=dict(
            brokers=[_address(n['address'], n['port']) for n in kafka_endpoints]
        ),
        admin_api=dict(
            addresses=[_address(n['private_ip'], c['admin_port']) for n in nodes]
        ),
    )
    for name in (
        'network',
        'disk_scheduler',
        'disk_nomerges',
        'disk_write_cache',
        'disk_irq',
        'cpu',
        'aio_events',
        'clocksource',
        'swappiness',
        'ballast_file',
    ):
        rpk['tune_' + name] = c.get('tune_' + name, True)
    node = dict(
        organization=c['organization'],
        cluster_id=c['cluster_id'],
        redpanda=broker,
        rpk=rpk,
        pandaproxy=dict(
            pandaproxy_api=[dict(address=ip, port=c['pandaproxy_port'])]
        ),
        schema_registry=dict(
            schema_registry_api=[
                dict(address=ip, port=c['schema_registry_port'])
            ],
            schema_registry_replication_factor=c.get(
                'schema_registry_replication_factor', 1
            ),
        ),
    )
    if c['enable_tls']:
        tls = dict(
            enabled=True,
            require_client_auth=c['tls'].get('require_client_auth', False),
            key_file=c['paths']['cert_directory'] + '/node.key',
            cert_file=c['paths']['cert_directory'] + '/node.crt',
            truststore_file=c['paths']['cert_directory'] + '/truststore.pem',
        )
        broker.update(
            admin_api_tls=[tls],
            rpc_server_tls=tls,
            kafka_api_tls=[dict(tls, name=x['name']) for x in listeners],
        )
        for component in ('pandaproxy', 'schema_registry'):
            node[component][component + '_api_tls'] = [tls]
        for api in ('admin_api', 'kafka_api'):
            rpk[api]['tls'] = dict(
                ca_file=tls['truststore_file'],
                cert_file=tls['cert_file'],
                key_file=tls['key_file'],
            )
    if c['kafka_enable_authorization']:
        sasl = c['sasl']
        cluster.update(
            superusers=sasl.get('superusers', [sasl.get('username', 'admin')]),
            admin_api_require_auth=c.get('admin_api_require_auth', True),
            http_authentication=c.get('http_authentication', ['BASIC']),
        )
        if c.get('schema_registry_enable_authorization'):
            cluster['schema_registry_enable_authorization'] = True
        rpk.update(
            sasl=dict(mechanism=sasl.get('mechanism', 'SCRAM-SHA-256')),
        )
    if c['enable_tls'] or c['kafka_enable_authorization']:
        for component in ('schema_registry', 'pandaproxy'):
            client = dict(brokers=copy.deepcopy(kafka_endpoints))
            if c['kafka_enable_authorization'] and c.get(
                'sasl_use_explicit_service_accounts'
            ):
                account = sasl[component]
                client.update(
                    scram_username=account['username'],
                    scram_password=account['password'],
                    sasl_mechanism='SCRAM-SHA-256',
                )
            if c['enable_tls']:
                client['broker_tls'] = dict(
                    enabled=True,
                    truststore_file=c['paths']['cert_directory'] + '/truststore.pem',
                )
                if c['tls'].get('require_client_auth'):
                    client['broker_tls'].update(
                        cert_file=tls['cert_file'], key_file=tls['key_file']
                    )
            node[component + '_client'] = client
            if c['kafka_enable_authorization']:
                node[component][component + '_api'][0]['authentication_method'] = c.get(
                    component + '_authn_method', 'http_basic'
                )
    if c['enable_fips']:
        broker.update(
            fips_mode=c.get('fips_mode', 'enabled'),
            openssl_config_file='/opt/redpanda/openssl/openssl.cnf',
            openssl_module_directory='/opt/redpanda/lib/ossl-modules/',
        )
    if c.get('tiered_storage'):
        cluster = layers.merge(cluster, c['tiered_storage'])
    result = layers.merge(
        dict(node=node, cluster=cluster), dict(node=c['node'], cluster=c['cluster'])
    )
    result = layers.merge(result, c['host_specific_override'])
    if any(key in result['node'].get('rpk', {}) for key in ('user', 'pass')):
        raise ValueError('Administrator credentials must come from pillar, not broker rpk config')
    for key, value in {
        'data_directory': c['data_directory'],
        'rpc_server': {'address': ip, 'port': c['rpc_port']},
        'advertised_rpc_api': {'address': ip, 'port': c['rpc_port']},
        'admin': [{'address': ip, 'port': c['admin_port']}],
        'kafka_api': listeners,
    }.items():
        if result['node']['redpanda'].get(key) != value:
            raise ValueError(
                'Set managed broker identity/path/listener settings through formula pillar, not node overrides: '
                + key
            )
    auth_user = c['sasl'].get('username', 'admin')
    if c['kafka_enable_authorization'] and c.get('sasl_use_explicit_service_accounts'):
        names = {
            c['sasl'][component]['username']
            for component in ('schema_registry', 'pandaproxy')
        }
        if names.intersection(result['cluster'].get('superusers', [])):
            raise ValueError('Service accounts cannot be cluster superusers')
        for component in ('schema_registry', 'pandaproxy'):
            client = result['node'].get(component + '_client', {})
            account = c['sasl'][component]
            if (
                client.get('scram_username') != account['username']
                or client.get('scram_password') != account['password']
            ):
                raise ValueError(
                    'Set managed service-account credentials through sasl, not node overrides'
                )
    if (
        c['kafka_enable_authorization']
        and result['cluster'].get('admin_api_require_auth')
        and (auth_user not in result['cluster'].get('superusers', []))
    ):
        raise ValueError(
            'Authenticated Admin API requires the managing user in superusers'
        )
    return result


def bootstrap_environment(ctx):
    c = ctx.config
    value = c['sasl'].get('username', 'admin') + ':' + c['sasl']['password']
    value += ':' + c['sasl'].get('mechanism', 'SCRAM-SHA-256')
    escaped = value.replace('\\', '\\\\').replace('"', '\\"')
    return 'RP_BOOTSTRAP_USER="' + escaped + '"'
