"""Reconcile cluster properties and license."""

import hashlib
import json
import tempfile
from pathlib import Path
from . import config, journal


def cluster_config(ctx, test=False):
    """Compare managed properties, patch the delta, then wait for every broker."""
    desired = config.configuration(ctx)['cluster']
    current = ctx.api('cluster_config?include_defaults=true')
    stamp = Path(ctx.path('work') + '/cluster-hashes.json')
    hashes = json.loads(stamp.read_text()) if stamp.exists() else {}
    managed_stamp = Path(ctx.path('work') + '/cluster-managed.json')
    managed = (
        json.loads(managed_stamp.read_text())
        if managed_stamp.exists()
        else list(hashes)
    )
    removed = sorted(set(managed) - set(desired))
    pending_stamp = Path(ctx.path('work') + '/cluster-pending.json')
    pending = json.loads(pending_stamp.read_text()) if pending_stamp.exists() else {}

    def digest(v):
        return hashlib.sha256(json.dumps(v, sort_keys=True).encode()).hexdigest()

    delta = {
        k: v
        for (k, v) in desired.items()
        if current.get(k) != v
        and (
            not (
                current.get(k) in ('[secret]', '[redacted]')
                and hashes.get(k) == digest(v)
            )
        )
    }
    if (delta or removed) and (not test):
        # Save ownership first: a lost HTTP reply must not lose our recovery state.
        journal.write_json(ctx, managed_stamp, sorted(set(managed) | set(desired)))
        journal.write_json(ctx, pending_stamp, pending)
        response = ctx.api('cluster_config', 'PUT', dict(upsert=delta, remove=removed))
        version = response['config_version']
        pending = {'config_version': version}
        journal.write_json(ctx, pending_stamp, pending)
        details = {'keys': sorted(delta), 'config_version': version}
        if removed:
            details['removed'] = removed
        ctx.record('cluster_config', details)
    if not test:
        # A controller-side no-op does not prove that all brokers accepted the config.
        wait_config(ctx, pending.get('config_version', 0))
        journal.write_json(ctx, stamp, {k: digest(v) for (k, v) in desired.items()})
        journal.write_json(ctx, managed_stamp, sorted(desired))
        pending_stamp.unlink(missing_ok=True)
    result = dict(changed=bool(delta or removed), keys=list(delta))
    if removed:
        result['removed'] = removed
    return result


def wait_config(ctx, version):

    def propagated():
        statuses = ctx.api('cluster_config/status')
        if (
            not isinstance(statuses, list)
            or not statuses
            or any((not isinstance(s, dict) for s in statuses))
        ):
            raise ValueError('Invalid cluster configuration status response')
        if any((s.get('invalid') or s.get('unknown') for s in statuses)):
            raise ValueError('Cluster configuration has invalid or unknown properties')
        ids = [s.get('node_id') for s in statuses]
        versions = [s.get('config_version') for s in statuses]
        if any((type(i) is not int for i in ids + versions)) or len(set(ids)) != len(
            ids
        ):
            raise ValueError('Invalid cluster configuration node IDs or versions')
        return (
            len(statuses) == len(ctx.config['nodes'])
            and len(set(versions)) == 1
            and (min(versions) >= version)
        )

    return ctx.wait(propagated, 'cluster configuration propagation')


def license_present(ctx, test=False):
    c = ctx.config
    if not c.get('license'):
        return dict(changed=False)
    digest = hashlib.sha256(c['license'].encode()).hexdigest()
    stamp = Path(ctx.path('work') + '/license-stamp.json')
    previous = json.loads(stamp.read_text()) if stamp.exists() else {}
    status = ctx.api('features/license')
    actual = status.get('license', {}).get('sha256')
    if status.get('loaded') and (
        actual == digest or previous == dict(desired=digest, actual=actual)
    ):
        return dict(changed=False)
    if not test:
        with tempfile.NamedTemporaryFile(
            mode='w', dir=ctx.path('work'), prefix='license-', suffix='.tmp'
        ) as source:
            source.write(c['license'])
            source.flush()
            ctx.run(
                [
                    'rpk',
                    'cluster',
                    'license',
                    'set',
                    '--path',
                    source.name,
                    '--config',
                    ctx.path('config'),
                ]
            )
        ctx.record('license', 'uploaded')
        status = ctx.api('features/license')
        if not status.get('loaded'):
            raise RuntimeError('Supplied license is not active')
        stamp.write_text(
            json.dumps(dict(desired=digest, actual=status['license']['sha256']))
        )
        stamp.chmod(0o600)
    return dict(changed=True)
