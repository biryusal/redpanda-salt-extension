"""Validate settings against supported platforms and live configuration."""

import os
import re
from .. import admin, security
from . import package


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
        if c['sasl'].get('mechanism', 'SCRAM-SHA-256') not in ('SCRAM-SHA-256', 'SCRAM-SHA-512'):
            raise ValueError('SASL mechanism must be SCRAM-SHA-256 or SCRAM-SHA-512')
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
        if names != set(package.packages(ctx)) or c['version'] == 'latest':
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
            [os.path.realpath(c['data_directory']), os.path.realpath(mountpoint)]
        ) != os.path.realpath(mountpoint):
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
