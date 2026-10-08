"""Enter maintenance, wait for drain and leave maintenance."""


from . import health, transaction


def maintenance_enable(ctx, test=False):

    def action(p):
        if not transaction._multi(ctx, p):
            return False
        status = ctx.api(f"brokers/{p['node_id']}").get('maintenance_status', {})
        if status.get('draining'):
            return False
        health.wait_healthy(ctx)
        ctx.api(f"brokers/{p['node_id']}/maintenance", 'PUT')
        ctx.record('maintenance', 'enabled')
        return True

    return transaction._step(ctx, 'maintenance', 'healthy_before_drain', test, action)


def maintenance_wait(ctx, test=False):

    def action(p):
        if transaction._multi(ctx, p):

            def drained():
                status = ctx.api(f"brokers/{p['node_id']}")['maintenance_status']
                if status.get('errors') or status.get('failed'):
                    raise ValueError('Maintenance drain failed')
                return status.get('finished', False)

            ctx.wait(drained, 'maintenance drain')
        return False

    return transaction._step(ctx, 'drained', 'maintenance', test, action)


def maintenance_disable(ctx, test=False):

    def action(p):
        if not transaction._multi(ctx, p):
            return False
        status = ctx.api(f"brokers/{p['node_id']}").get('maintenance_status', {})
        if not status.get('draining'):
            return False
        ctx.wait(lambda: health.local_ready(ctx), 'local readiness before leaving maintenance')
        ctx.api(f"brokers/{p['node_id']}/maintenance", 'DELETE')
        ctx.record('maintenance', 'disabled')
        return True

    return transaction._step(ctx, 'maintenance_cleared', 'ready', test, action)
