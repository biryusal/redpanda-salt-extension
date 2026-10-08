"""Create internal client accounts without overwriting existing passwords."""

import hashlib
import json
from pathlib import Path
from .. import admin
from . import validate


def service_accounts(ctx, test=False):
    """Create internal client accounts; never overwrite an existing password."""
    c = ctx.config
    if not c['kafka_enable_authorization'] or not c.get(
        'sasl_use_explicit_service_accounts'
    ):
        return dict(changed=False)
    live = admin.live_config(ctx)
    validate.validate_service_accounts(ctx, c, live)
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
                (live.get(other + '_client') or {}).get('scram_username') == username
                and (live.get(other + '_client') or {}).get('scram_password')
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
