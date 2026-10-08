"""Reconcile application users and explicitly requested password updates."""

import hashlib
import json
import urllib.parse
from pathlib import Path
from .. import admin, journal
from . import validate


def users_managed(ctx, test=False):
    """Create/delete application users; rotate passwords only when explicitly enabled."""
    c = ctx.config
    validate.validate_security(ctx, test=test)
    validate.validate_users(ctx, c, admin.live_config(ctx))
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
