"""Check readiness, cluster health and restart safety at each lifecycle gate."""

import json
from . import transaction


def restart_safety(ctx, node_id):
    """Fail closed on an unhealthy cluster, incomplete drain or partition risk."""
    if not healthy(ctx):
        raise RuntimeError('Cluster became unhealthy before restart')
    status = ctx.api(f'brokers/{node_id}').get('maintenance_status', {})
    if (
        not status.get('draining')
        or not status.get('finished')
        or status.get('errors')
        or status.get('failed')
    ):
        raise RuntimeError('Broker must finish maintenance drain before restart')
    if ctx.config['restart_probe']:
        risks = ctx.api('broker/pre_restart_probe', local=True).get('risks')
        required = {
            'rf1_offline',
            'full_acks_produce_unavailable',
            'unavailable',
            'acks1_data_loss',
        }
        if (
            not isinstance(risks, dict)
            or not required.issubset(risks)
            or any((not isinstance(v, list) for v in risks.values()))
        ):
            raise RuntimeError('Invalid pre-restart probe response')
        allowed = set(ctx.config['allowed_restart_risks'])
        if not allowed.issubset(required):
            raise ValueError('Unknown allowed restart risk')
        blocked = sorted(
            (k for (k, value) in risks.items() if value and k not in allowed)
        )
        if blocked:
            details = {
                risk: {'count': len(risks[risk]), 'sample': risks[risk][:5]}
                for risk in blocked
            }
            raise RuntimeError(
                'Restart partition risks: '
                + json.dumps(details, sort_keys=True)
            )
    return True


def healthy(ctx):
    h = ctx.api('cluster/health_overview')
    if (
        not isinstance(h, dict)
        or h.get('is_healthy') is not True
        or any(
            (
                h.get(k)
                for k in (
                    'nodes_down',
                    'leaderless_partitions',
                    'under_replicated_partitions',
                )
            )
        )
    ):
        return False
    brokers = ctx.api('brokers')
    expected = {n['private_ip'] for n in ctx.config['nodes'].values()}
    if (
        not isinstance(brokers, list)
        or len(brokers) != len(expected)
        or any((not isinstance(b, dict) for b in brokers))
    ):
        return False
    addresses = [b.get('internal_rpc_address') for b in brokers]
    ids = [b.get('node_id') for b in brokers]
    reported = h.get('all_nodes')
    return bool(
        all((isinstance(a, str) for a in addresses))
        and set(addresses) == expected
        and all((type(i) is int for i in ids))
        and (len(set(ids)) == len(ids))
        and isinstance(reported, list)
        and all((type(i) is int for i in reported))
        and (len(reported) == len(ids))
        and (set(reported) == set(ids))
    )


def wait_healthy(ctx):
    return ctx.wait(lambda: healthy(ctx), 'healthy cluster with all expected brokers')


def local_ready(ctx):
    status = ctx.api('status/ready', local=True)
    return isinstance(status, dict) and status.get('status') == 'ready'


def health_before_drain(ctx, test=False):

    def action(p):
        if transaction._multi(ctx, p):
            ctx.wait(lambda: local_ready(ctx), 'existing local broker readiness')
            wait_healthy(ctx)
        return False

    return transaction._step(ctx, 'healthy_before_drain', 'restored', test, action)


def safety_after_drain(ctx, test=False):

    def action(p):
        if transaction._multi(ctx, p):
            last_error = None

            def safe():
                nonlocal last_error
                try:
                    return restart_safety(ctx, p['node_id'])
                except RuntimeError as exc:
                    last_error = str(exc)
                    raise

            try:
                ctx.wait(safe, 'safe broker restart after drain')
            except RuntimeError as exc:
                if last_error:
                    raise RuntimeError(f'{exc}; last check: {last_error}') from None
                raise
        return False

    return transaction._step(ctx, 'safe_to_update', 'drained', test, action)


def health_after_start(ctx, test=False):

    def action(p):
        if p['initialized']:
            wait_healthy(ctx)
        return False

    return transaction._step(ctx, 'healthy_after_start', 'maintenance_cleared', test, action)


def readiness_wait(ctx, test=False):

    def action(p):
        ctx.wait(lambda: local_ready(ctx), 'local broker readiness')
        return False

    return transaction._step(ctx, 'ready', 'started', test, action)
