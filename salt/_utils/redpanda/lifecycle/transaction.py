"""Persist lifecycle checkpoints; SLS defines the execution order."""

import json
from pathlib import Path
from .. import admin, journal
from . import health, planning


def _pending(ctx, test=False):
    if journal.pending_path(ctx).exists():
        return json.loads(journal.pending_path(ctx).read_text())
    if test:
        desired_plan = planning.plan(ctx)
        return desired_plan if desired_plan['needed'] else None
    return None


def _multi(ctx, pending):
    return pending['initialized'] and len(ctx.config['nodes']) > 1


def _step(ctx, name, previous, test, action):
    """Check prerequisite, perform one action, then persist its checkpoint."""
    pending = _pending(ctx, test)
    if pending is None:
        return {'changed': False}
    if test:
        return {'changed': True, 'phase': name}
    steps = pending.setdefault('steps', [])
    if previous not in steps:
        raise RuntimeError('Lifecycle phase ' + name + ' requires ' + previous)
    # Safety assertions run again even when a checkpoint already exists.
    changed = action(pending)
    if name not in steps:
        steps.append(name)
        journal.write_pending(ctx, pending)
    return {'changed': bool(changed), 'phase': name}


def transaction_prepare(ctx, test=False):
    """Plan and open/reset a transaction; never drain or change services."""
    desired_plan = planning.plan(ctx)
    if (
        desired_plan['initialized']
        and desired_plan['needed']
        and (not ctx.config['restart_node'])
    ):
        raise RuntimeError(
            'Change requires a restart but restart_node is false; no files or packages were changed'
        )
    if test or not desired_plan['needed']:
        return desired_plan
    desired_plan['admin_addresses'] = desired_plan.get(
        'admin_addresses', admin.live_addresses(ctx)
    )
    # A new apply repeats the SLS gates instead of trusting previous health checks.
    desired_plan['steps'] = ['prepared']
    journal.write_pending(ctx, desired_plan)
    ctx.record(
        'pending',
        {
            'initialized': desired_plan['initialized'],
            'node_id': desired_plan['node_id'],
        },
    )
    return desired_plan


def transaction_complete(ctx, test=False):
    pending = _pending(ctx, test)
    if pending is None:
        return {'changed': False}
    if test:
        return {'changed': True}
    if 'healthy_after_start' not in pending.get('steps', []):
        raise RuntimeError('Cannot complete transaction before final health check')
    ctx.wait(lambda: health.local_ready(ctx), 'readiness before completing transaction')
    if pending['initialized']:
        health.wait_healthy(ctx)
    Path(ctx.path('bootstrap_env')).unlink(missing_ok=True)
    journal.pending_path(ctx).unlink()
    ctx.record('pending', 'cleared')
    return {'changed': True}
