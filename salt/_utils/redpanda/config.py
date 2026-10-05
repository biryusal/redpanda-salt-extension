"""Desired configuration: merge, validate, render."""

import os
import re
from . import admin, lifecycle, security
import copy


def merge(a, b):
    result = copy.deepcopy(a)
    for key, value in b.items():
        result[key] = (
            merge(result[key], value)
            if isinstance(value, dict) and isinstance(result.get(key), dict)
            else copy.deepcopy(value)
        )
    return result


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
        remote = merge(inventory_config or c, n.get('overrides', {}))
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
            sasl=dict(mechanism='SCRAM-SHA-256'),
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
        cluster = merge(cluster, c['tiered_storage'])
    result = merge(
        dict(node=node, cluster=cluster), dict(node=c['node'], cluster=c['cluster'])
    )
    result = merge(result, c['host_specific_override'])
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


def validate(ctx):
    c = ctx.config
    security.validate_transport(c)
    security.validate_service_accounts(ctx, c)
    for path in c['paths'].values():
        if (
            not isinstance(path, str)
            or not os.path.isabs(path)
            or any((x in path for x in ('\n', '\r', '\x00')))
        ):
            raise ValueError(
                'Managed paths must be absolute paths without control characters'
            )
    if ctx.grains.get('os_family') not in ('Debian', 'RedHat'):
        raise ValueError('Only Debian and RedHat OS families are supported')
    if not c['version'] or not c['nodes'] or ctx.minion_id not in c['nodes']:
        raise ValueError('version and nodes keyed by minion ID are required')
    if (
        not isinstance(c['data_directory'], str)
        or not os.path.isabs(c['data_directory'])
        or any((x in c['data_directory'] for x in ('\n', '\r', '\x00')))
    ):
        raise ValueError(
            'data_directory must be an absolute path without control characters'
        )
    for name in (
        'rpc_port',
        'admin_port',
        'kafka_port',
        'pandaproxy_port',
        'schema_registry_port',
    ):
        if type(c[name]) is not int or not 1 <= c[name] <= 65535:
            raise ValueError(name + ' must be an integer port')
    for name in ('user', 'group'):
        if not isinstance(c[name], str) or not re.fullmatch(
            '[a-z_][a-z0-9_-]*[$]?', c[name]
        ):
            raise ValueError(name + ' must be a system account name')
    addresses = [node.get('private_ip') for node in c['nodes'].values()]
    if len(addresses) != len(set(addresses)):
        raise ValueError('Broker private_ip addresses must be unique')
    live = admin.live_config(ctx)
    security.validate_service_accounts(ctx, c, live)
    old_path = live.get('redpanda', {}).get('data_directory')
    if (
        old_path
        and old_path != c['data_directory']
        and os.path.isdir(old_path + '/redpanda/controller')
    ):
        raise ValueError(
            'Changing an initialized data directory requires a separate data migration'
        )
    if c['install_status'] not in ('present', 'latest'):
        raise ValueError('install_status must be present or latest')
    if c['enable_fips'] and ctx.grains['os_family'] != 'RedHat':
        raise ValueError('FIPS packages are supported only on RedHat')
    for node in c['nodes'].values():
        if not node.get('private_ip'):
            raise ValueError('Every node needs private_ip')
    if c['kafka_enable_authorization']:
        password = c['sasl'].get('password')
        username = c['sasl'].get('username', 'admin')
        if not isinstance(password, str) or not password:
            raise ValueError('SASL requires an explicit bootstrap password')
        if any((x in password for x in ('\n', '\r', '\x00'))) or any(
            (x in username for x in (':', '\n', '\r', '\x00'))
        ):
            raise ValueError('Bootstrap credentials contain unsupported characters')
    if c['enable_tls']:
        if not all(
            (c['tls'].get(k) for k in ('ca_source', 'cert_source', 'key_source'))
        ):
            raise ValueError('TLS requires ca_source, cert_source and key_source')
    if c.get('sasl_force_clear_data_directory'):
        raise ValueError(
            'Automatic deletion of cluster data is intentionally unsupported'
        )
    if c['development_build']:
        if (
            not c.get('nightly_repository')
            or not c.get('nightly_key_url')
            or (ctx.grains['os_family'] == 'RedHat' and (not c.get('nightly_baseurl')))
        ):
            raise ValueError('Nightly requires nightly_repository and nightly_key_url')
    if c['airgap']:
        sources = c.get('airgap_sources', [])
        names = {next(iter(item)) for item in sources}
        if names != set(packages(ctx)) or c['version'] == 'latest':
            raise ValueError(
                'Airgap requires pinned version and one source per required package'
            )
    if c['storage']['devices']:
        mountpoint = c['storage'].get('mountpoint', '/mnt/vectorized')
        if (
            not isinstance(mountpoint, str)
            or not os.path.isabs(mountpoint)
            or os.path.normpath(mountpoint) == '/'
        ):
            raise ValueError('Storage mountpoint must be an absolute non-root path')
        if os.path.commonpath(
            [os.path.abspath(c['data_directory']), os.path.abspath(mountpoint)]
        ) != os.path.abspath(mountpoint):
            raise ValueError(
                'data_directory must be inside the managed storage mountpoint'
            )
        if not c['storage'].get('uuid'):
            raise ValueError('Managed storage requires an explicit filesystem UUID')
        for device in c['storage']['devices']:
            if not re.fullmatch('/dev/[A-Za-z0-9_./-]+', device) or device in (
                '/dev/sda',
                '/dev/vda',
            ):
                raise ValueError('Storage must use explicit non-root devices')
    if c['storage']['devices'] and (
        not re.fullmatch('[0-9a-fA-F-]{36}', c['storage']['uuid'])
    ):
        raise ValueError('Storage UUID must be a filesystem UUID')
    if c.get('sasl_use_explicit_service_accounts'):
        for name in ('schema_registry', 'pandaproxy'):
            if not all(
                (c['sasl'].get(name, {}).get(k) for k in ('username', 'password'))
            ):
                raise ValueError(
                    'Explicit service accounts require username and password'
                )
    return True


def packages(ctx):
    c = ctx.config
    split = c['version'] == 'latest' or tuple(
        (int(x) for x in c['version'].split('-')[0].split('.')[:2])
    ) >= (24, 2)
    names = ['redpanda', 'redpanda-rpk', 'redpanda-tuner'] if split else ['redpanda']
    if c['enable_fips']:
        names += ['redpanda-fips', 'redpanda-rpk-fips']
    return names


def configuration(ctx):
    validate(ctx)
    return build(ctx.config, ctx.minion_id, ctx.inventory)


def initialized(ctx):
    return os.path.isdir(ctx.config['data_directory'] + '/redpanda/controller')


def bootstrap_environment(ctx):
    c = ctx.config
    value = c['sasl'].get('username', 'admin') + ':' + c['sasl']['password']
    escaped = value.replace('\\', '\\\\').replace('"', '\\"')
    return 'RP_BOOTSTRAP_USER="' + escaped + '"'


def validate_packages(ctx):
    c = ctx.config
    if c['version'] != 'latest' and lifecycle.package_changes(ctx):
        raise ValueError('Installed broker packages do not match the requested version')
    return True
