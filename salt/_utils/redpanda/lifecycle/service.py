"""Restore, mask and start broker services with safety checks."""

from .. import config, journal
from . import health, transaction


def pending_restore(ctx, test=False):

    def action(p):
        if not p['initialized'] or ctx.salt['service.status']('redpanda'):
            return False
        ctx.run(['systemctl', 'unmask', '--runtime', 'redpanda.service'])
        ctx.run(['systemctl', 'daemon-reload'])
        if not ctx.salt['service.start']('redpanda'):
            raise RuntimeError('Cannot restore interrupted broker before drain')
        ctx.record('service', 'restored')
        return True

    return transaction._step(ctx, 'restored', 'prepared', test, action)


def service_mask(ctx, test=False):

    def action(p):
        ctx.run(['systemctl', 'mask', '--runtime', 'redpanda.service'])
        ctx.record('runtime_mask', True)
        return True

    return transaction._step(ctx, 'masked', 'safe_to_update', test, action)


def safety_before_restart(ctx, test=False):

    def action(p):
        if ctx.salt['service.masked']('redpanda', runtime=True):
            raise RuntimeError(
                'Broker is still runtime masked; unmask must succeed before restart'
            )
        if transaction._multi(ctx, p) and ctx.salt['service.status']('redpanda'):
            health.restart_safety(ctx, p['node_id'])
        return False

    return transaction._step(ctx, 'safe_to_restart', 'masked', test, action)


def broker_start(ctx, test=False):

    def action(p):
        running = ctx.salt['service.status']('redpanda')
        if 'started' in p['steps'] and running:
            return False
        if transaction._multi(ctx, p) and running:
            health.restart_safety(ctx, p['node_id'])
        fun = 'service.restart' if p['initialized'] and running else 'service.start'
        if not ctx.salt[fun]('redpanda'):
            raise RuntimeError('Failed to start/restart redpanda')
        ctx.record('service', fun.split('.')[-1])
        return True

    return transaction._step(ctx, 'started', 'safe_to_restart', test, action)


def restore_service(ctx, test=False):
    if (
        not config.initialized(ctx)
        or journal.pending_path(ctx).exists()
        or ctx.salt['service.status']('redpanda')
    ):
        return dict(changed=False)
    if not test:
        if not ctx.salt['service.start']('redpanda'):
            raise RuntimeError('Failed to restore existing broker service')
        ctx.record('service', 'restored')
        ctx.wait(lambda: health.local_ready(ctx), 'existing broker readiness')
    return dict(changed=True)
