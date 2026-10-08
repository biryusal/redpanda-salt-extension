"""Validate transport, protected identities and desired users/ACLs."""

import os
from .. import admin, journal


def administrators(c):
    """Protect administrators declared through every supported override layer."""
    names = {c['sasl'].get('username', 'admin')}
    names.update(c['sasl'].get('superusers', []))
    names.update(c.get('cluster', {}).get('superusers', []))
    names.update(c.get('host_specific_override', {}).get('cluster', {}).get('superusers', []))
    return names


def validate_transport(c):
    if type(c.get('allow_insecure_sasl', False)) is not bool:
        raise ValueError('allow_insecure_sasl must be a boolean')
    if c['kafka_enable_authorization'] and not c['enable_tls'] and not c.get('allow_insecure_sasl', False):
        raise ValueError('SASL requires TLS; allow_insecure_sasl=true is only for isolated test networks')


def validate_service_accounts(ctx, c, live=None):
    if not c.get('sasl_use_explicit_service_accounts'):
        return
    if not c['kafka_enable_authorization']:
        raise ValueError('Explicit service accounts require kafka_enable_authorization')
    names = administrators(c)
    for component in ('schema_registry', 'pandaproxy'):
        account = c['sasl'].get(component, {})
        name, password = (account.get('username'), account.get('password'))
        if (
            not isinstance(name, str)
            or not name
            or any((ord(ch) < 32 or ord(ch) == 127 for ch in name))
            or (not isinstance(password, str))
            or (not password)
        ):
            raise ValueError(
                'Explicit service accounts require valid username and password'
            )
        if name in names:
            raise ValueError(
                'Service accounts must be distinct from administrators/superusers and each other'
            )
        names.add(name)
        for other in ('schema_registry', 'pandaproxy'):
            previous = (live or {}).get(other + '_client') or {}
            if (
                previous.get('scram_username') == name
                and previous.get('scram_password') != password
            ):
                raise ValueError(
                    'Service-account password rotation requires a new username; retain the old account until rollout completes'
                )


def validate_users(ctx, c, live=None):
    validate_service_accounts(ctx, c)
    users = c.get('sasl_users', [])
    if not isinstance(users, list):
        raise ValueError('sasl_users must be a list')
    if users and (not c['kafka_enable_authorization']):
        raise ValueError('sasl_users requires kafka_enable_authorization')
    reserved = administrators(c)
    if c.get('sasl_use_explicit_service_accounts'):
        reserved.update(
            (
                c['sasl'].get(component, {}).get('username')
                for component in ('schema_registry', 'pandaproxy')
            )
        )
    reserved.update(
        (
            ((live or {}).get(component + '_client') or {}).get('scram_username')
            for component in ('schema_registry', 'pandaproxy')
        )
    )
    names = set()
    for user in users:
        if not isinstance(user, dict):
            raise ValueError('Every sasl_users entry must be a mapping')
        name = user.get('username')
        if (
            not isinstance(name, str)
            or not name
            or any((ord(ch) < 32 or ord(ch) == 127 for ch in name))
        ):
            raise ValueError(
                'Usernames must be nonempty strings without control characters'
            )
        if name in names:
            raise ValueError('sasl_users usernames must be unique')
        names.add(name)
        if name in reserved:
            raise ValueError(
                'sasl_users cannot manage the administrator or managed service accounts'
            )
        if user.get('state', 'present') not in ('present', 'absent'):
            raise ValueError('User state must be present or absent')
        if type(user.get('update_password', False)) is not bool:
            raise ValueError('update_password must be a boolean')
        if user.get('mechanism', 'SCRAM-SHA-256') not in (
            'SCRAM-SHA-256',
            'SCRAM-SHA-512',
        ):
            raise ValueError('User mechanism must be SCRAM-SHA-256 or SCRAM-SHA-512')
        if user.get('state', 'present') == 'present' and (
            not isinstance(user.get('password'), str) or not user['password']
        ):
            raise ValueError('Present users require a nonempty password')


def validate_acls(ctx, c, live=None):
    acls = c.get('sasl_acls', [])
    if not isinstance(acls, list):
        raise ValueError('sasl_acls must be a list')
    if acls and (not c['kafka_enable_authorization']):
        raise ValueError('sasl_acls requires kafka_enable_authorization')
    protected = administrators(c)
    protected.update(
        (
            ((live or {}).get(component + '_client') or {}).get('scram_username')
            for component in ('schema_registry', 'pandaproxy')
        )
    )
    if c.get('sasl_use_explicit_service_accounts'):
        protected.update(
            (
                c['sasl'][component]['username']
                for component in ('schema_registry', 'pandaproxy')
            )
        )
    absent_users = {
        u['username'] for u in c.get('sasl_users', []) if u.get('state') == 'absent'
    }
    seen = set()
    for acl in acls:
        if not isinstance(acl, dict) or set(acl) - {
            'username',
            'resource_type',
            'resource_name',
            'operation',
            'permission',
            'host',
            'pattern',
            'state',
        }:
            raise ValueError(
                'Every sasl_acls entry must be a mapping with supported fields'
            )
        for key in ('username', 'resource_name', 'host'):
            value = acl.get(key, '*' if key == 'host' else None)
            if (
                not isinstance(value, str)
                or not value
                or ',' in value
                or any((ord(ch) < 32 or ord(ch) == 127 for ch in value))
            ):
                raise ValueError(
                    'ACL names and hosts must be nonempty strings without commas or control characters'
                )
        if (
            acl['username'] in protected
            or acl['username'] == '*'
            or acl['username'].startswith(('User:', 'RedpandaRole:'))
        ):
            raise ValueError(
                'ACL username must identify an application user, not administrators, service accounts or wildcard principals'
            )
        if acl.get('resource_type') not in (
            'topic',
            'group',
            'cluster',
            'transactional_id',
        ):
            raise ValueError('Unsupported ACL resource_type')
        if acl['resource_type'] == 'cluster' and (
            acl['resource_name'] != 'kafka-cluster'
            or acl.get('pattern', 'literal') != 'literal'
        ):
            raise ValueError(
                'Cluster ACLs require resource_name kafka-cluster and literal pattern'
            )
        if acl.get('operation') not in (
            'all',
            'read',
            'write',
            'create',
            'delete',
            'alter',
            'describe',
            'describe_configs',
            'alter_configs',
            'cluster_action',
            'idempotent_write',
        ):
            raise ValueError('Unsupported ACL operation')
        if (
            acl.get('permission', 'allow') not in ('allow', 'deny')
            or acl.get('pattern', 'literal') not in ('literal', 'prefixed')
            or acl.get('state', 'present') not in ('present', 'absent')
        ):
            raise ValueError('Invalid ACL permission, pattern or state')
        if acl.get('state', 'present') == 'present' and acl['username'] in absent_users:
            raise ValueError('Cannot grant an ACL to a user declared absent')
        identity = tuple(
            (
                acl.get(k, default)
                for (k, default) in (
                    ('username', None),
                    ('resource_type', None),
                    ('resource_name', None),
                    ('operation', None),
                    ('permission', 'allow'),
                    ('host', '*'),
                    ('pattern', 'literal'),
                )
            )
        )
        if identity in seen:
            raise ValueError('sasl_acls rules must be unique')
        seen.add(identity)


def validate_security(ctx, test=False):
    """Preflight only; no package/version/configuration changes are required."""
    c = ctx.config
    validate_transport(c)
    if not c['nodes'] or ctx.minion_id not in c['nodes']:
        raise ValueError('Pass the complete nodes inventory keyed by minion ID')
    if journal.pending_path(ctx).exists():
        raise RuntimeError(
            'Pending broker deployment must be recovered before managing users/ACLs'
        )
    live = admin.live_config(ctx)
    validate_users(ctx, c, live)
    validate_acls(ctx, c, live)
    if c['kafka_enable_authorization'] and (
        not isinstance(c['sasl'].get('password'), str) or not c['sasl']['password']
    ):
        raise ValueError(
            'Security management requires administrator credentials in sasl'
        )
    for name in ('work', 'config', 'cert_directory'):
        path = c['paths'][name]
        if (
            not isinstance(path, str)
            or not os.path.isabs(path)
            or any((ch in path for ch in ('\n', '\r', '\x00')))
        ):
            raise ValueError(
                'Security paths must be absolute without control characters'
            )
    return {'changed': False}
