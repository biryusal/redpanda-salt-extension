"""Build, compare and reconcile exact Kafka ACL rules."""

import json
import re
from .. import admin, config
from . import validate


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
    validate.validate_security(ctx, test=test)
    live = admin.live_config(ctx)
    validate.validate_users(ctx, c, live)
    validate.validate_acls(ctx, c, live)
    if not c.get('sasl_acls'):
        return {'changed': False}
    cfg = config.build(c, ctx.minion_id, ctx.inventory)['node']['rpk']
    connection = [
        '--config',
        ctx.path('config'),
        '-X',
        'brokers=' + ','.join(cfg['kafka_api']['brokers']),
        '-X',
        'sasl.mechanism=' + c['sasl'].get('mechanism', 'SCRAM-SHA-256'),
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
