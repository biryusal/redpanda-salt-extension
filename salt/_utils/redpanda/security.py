"""Reconcile declared SCRAM users and Kafka ACLs."""

import hashlib
import json
import os
import re
import urllib.parse
from pathlib import Path
from . import admin, config, journal


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
            previous = (live or {}).get(other + '_client', {})
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
            (live or {}).get(component + '_client', {}).get('scram_username')
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


def users_managed(ctx, test=False):
    """Create/delete application users; rotate passwords only when explicitly enabled."""
    c = ctx.config
    validate_security(ctx, test=test)
    validate_users(ctx, c, admin.live_config(ctx))
    desired = c.get('sasl_users', [])
    if not desired:
        return dict(changed=False)
    existing = ctx.api('security/users')
    if not isinstance(existing, list) or any(
        (not isinstance(name, str) for name in existing)
    ):
        raise RuntimeError('Invalid user-list response')
    existing = set(existing)
    stamp = Path(ctx.path('work') + '/users-hashes.json')
    hashes = json.loads(stamp.read_text()) if stamp.exists() else {}
    changes = []
    for user in desired:
        name = user['username']
        path = 'security/users/' + urllib.parse.quote(name, safe='')
        if user.get('state', 'present') == 'absent':
            if name in existing:
                if not test:
                    ctx.api(path, 'DELETE')
                    existing.remove(name)
                changes.append(dict(username=name, action='deleted'))
                if not test:
                    ctx.record('users', list(changes))
            if not test and name in hashes:
                del hashes[name]
                journal.write_json(ctx, stamp, hashes)
            continue
        mechanism = user.get('mechanism', 'SCRAM-SHA-256')
        digest = hashlib.sha256(
            json.dumps([name, mechanism, user['password']]).encode()
        ).hexdigest()
        create = name not in existing
        rotate = user.get('update_password', False) and hashes.get(name) != digest
        if not create and (not rotate):
            continue
        if not test:
            payload = dict(password=user['password'], algorithm=mechanism)
            if create:
                ctx.api('security/users', 'POST', dict(payload, username=name))
                existing.add(name)
            else:
                ctx.api(path, 'PUT', payload)
        changes.append(dict(username=name, action='created' if create else 'updated'))
        if not test:
            ctx.record('users', list(changes))
            hashes[name] = digest
            journal.write_json(ctx, stamp, hashes)
    return dict(changed=bool(changes), users=changes)


def validate_acls(ctx, c, live=None):
    acls = c.get('sasl_acls', [])
    if not isinstance(acls, list):
        raise ValueError('sasl_acls must be a list')
    if acls and (not c['kafka_enable_authorization']):
        raise ValueError('sasl_acls requires kafka_enable_authorization')
    protected = administrators(c)
    protected.update(
        (
            (live or {}).get(component + '_client', {}).get('scram_username')
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


def acl_flags(ctx, acl):
    permission = acl.get('permission', 'allow')
    flags = [
        '--' + permission + '-principal',
        'User:' + acl['username'],
        '--' + permission + '-host',
        acl.get('host', '*'),
        '--operation',
        acl['operation'].replace('_', '-'),
        '--resource-pattern-type',
        acl.get('pattern', 'literal'),
    ]
    if acl['resource_type'] == 'cluster':
        flags.append('--cluster')
    else:
        flags += ['--' + acl['resource_type'].replace('_', '-'), acl['resource_name']]
    return flags


def acl_matches(ctx, acl, flags, connection, env):
    value = json.loads(
        ctx.run(
            ['rpk', 'security', 'acl', 'list', '--format', 'json'] + flags + connection,
            env=env,
        )
    )
    if isinstance(value, dict):
        filters = value.get('filters', [])
        if not isinstance(filters, list) or any(
            (not isinstance(f, dict) or f.get('message') for f in filters)
        ):
            raise RuntimeError('ACL lookup failed')
        if 'matches' not in value:
            raise RuntimeError('Invalid ACL-list response')
        value = value['matches']
    if value is None:
        return []
    if not isinstance(value, list) or any(
        (not isinstance(item, dict) for item in value)
    ):
        raise RuntimeError('Invalid ACL-list response')
    expected = {
        'principal': 'User:' + acl['username'],
        'host': acl.get('host', '*'),
        'resource_type': acl['resource_type'],
        'resource_name': acl['resource_name'],
        'resource_pattern_type': acl.get('pattern', 'literal'),
        'operation': acl['operation'],
        'permission': acl.get('permission', 'allow'),
    }

    def normalized(key, value):
        return (
            re.sub('[^a-z]', '', value.lower())
            if key
            in ('resource_type', 'resource_pattern_type', 'operation', 'permission')
            else value
        )

    if any(
        (
            any(
                (
                    not isinstance(row.get(key), str)
                    or normalized(key, row[key]) != normalized(key, target)
                    for (key, target) in expected.items()
                )
            )
            for row in value
        )
    ):
        raise RuntimeError(
            'ACL filter returned a different rule; refusing a broad mutation'
        )
    return value


def acls_managed(ctx, test=False):
    """Manage declared Kafka ACLs; retain every undeclared rule."""
    c = ctx.config
    validate_security(ctx, test=test)
    live = admin.live_config(ctx)
    validate_users(ctx, c, live)
    validate_acls(ctx, c, live)
    if not c.get('sasl_acls'):
        return {'changed': False}
    cfg = config.build(c, ctx.minion_id, ctx.inventory)['node']['rpk']
    connection = [
        '--config',
        ctx.path('config'),
        '-X',
        'brokers=' + ','.join(cfg['kafka_api']['brokers']),
        '-X',
        'sasl.mechanism=SCRAM-SHA-256',
        '-X',
        'tls.enabled=' + str(c['enable_tls']).lower(),
    ]
    if c['enable_tls']:
        connection += ['-X', 'tls.ca=' + cfg['kafka_api']['tls']['ca_file']]
        if c['tls'].get('require_client_auth'):
            connection += [
                '-X',
                'tls.cert=' + cfg['kafka_api']['tls']['cert_file'],
                '-X',
                'tls.key=' + cfg['kafka_api']['tls']['key_file'],
            ]
    env = {
        'RPK_USER': c['sasl'].get('username', 'admin'),
        'RPK_PASS': c['sasl']['password'],
    }
    changes = []
    for acl in c['sasl_acls']:
        flags = acl_flags(ctx, acl)
        present = bool(acl_matches(ctx, acl, flags, connection, env))
        desired = acl.get('state', 'present') == 'present'
        if present == desired:
            continue
        action = 'create' if desired else 'delete'
        if not test:
            ctx.run(
                ['rpk', 'security', 'acl', action]
                + (['--no-confirm'] if not desired else [])
                + flags
                + connection,
                env=env,
            )
            change = dict(acl, action=action)
            changes.append(change)
            ctx.record('acls', list(changes))
            if bool(acl_matches(ctx, acl, flags, connection, env)) != desired:
                raise RuntimeError('ACL operation did not reach the desired state')
        else:
            changes.append(dict(acl, action=action))
    return {'changed': bool(changes), 'acls': changes}


def service_accounts(ctx, test=False):
    """Create internal client accounts; never overwrite an existing password."""
    c = ctx.config
    if not c['kafka_enable_authorization'] or not c.get(
        'sasl_use_explicit_service_accounts'
    ):
        return dict(changed=False)
    live = admin.live_config(ctx)
    validate_service_accounts(ctx, c, live)
    users = ctx.api('security/users')
    if not isinstance(users, list) or any(
        (not isinstance(name, str) for name in users)
    ):
        raise RuntimeError('Invalid user-list response')
    users = set(users)
    accounts = []
    for component in ('schema_registry', 'pandaproxy'):
        account = c['sasl'][component]
        username = account['username']
        digest = hashlib.sha256(
            json.dumps([username, account['password']]).encode()
        ).hexdigest()
        stamp = Path(ctx.path('work') + '/' + component + '-user.sha256')
        previous = stamp.read_text() if stamp.exists() else None
        configured = any(
            (
                live.get(other + '_client', {}).get('scram_username') == username
                and live.get(other + '_client', {}).get('scram_password')
                == account['password']
                for other in ('schema_registry', 'pandaproxy')
            )
        )
        if username in users and previous != digest and (not configured):
            raise ValueError(
                'Existing service-account credentials cannot be verified; use a new username for migration'
            )
        accounts.append((component, account, digest, stamp))
    changed = False
    for component, account, digest, stamp in accounts:
        username = account['username']
        if username not in users:
            changed = True
            if not test:
                ctx.api(
                    'security/users',
                    'POST',
                    dict(
                        username=username,
                        password=account['password'],
                        algorithm='SCRAM-SHA-256',
                    ),
                )
                users.add(username)
                ctx.record(
                    'account_' + component, {'username': username, 'action': 'created'}
                )
        if not test:
            stamp.write_text(digest)
            stamp.chmod(0o600)
        for resource in (['--topic', '*'], ['--cluster']):
            base = ['rpk', 'security', 'acl']
            flags = (
                [
                    '--allow-principal',
                    'User:' + username,
                    '--allow-host',
                    '*',
                    '--operation',
                    'all',
                    '--resource-pattern-type',
                    'literal',
                ]
                + resource
                + ['--config', ctx.path('config')]
            )
            acls = json.loads(ctx.run(base + ['list', '--format', 'json'] + flags))
            if isinstance(acls, dict):
                if any((f.get('message') for f in acls.get('filters', []))):
                    raise RuntimeError('ACL lookup failed')
                acls = acls['matches']
            if not acls:
                changed = True
                if not test:
                    ctx.run(base + ['create'] + flags)
                    ctx.record('acl_' + component + '_' + resource[0], 'created')
    return dict(changed=changed)
