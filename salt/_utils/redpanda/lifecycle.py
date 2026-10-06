"""One broker operation at a time; SLS owns execution order."""

import json
from pathlib import Path
from . import admin, config, journal


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


def package_changes(ctx):
    c = ctx.config
    current = ctx.salt['pkg.version'](*config.packages(ctx))
    if isinstance(current, str):
        current = {config.packages(ctx)[0]: current}
    if c['version'] != 'latest':
        return any((current.get(p) != c['version'] for p in config.packages(ctx)))
    if any((not current.get(p) for p in config.packages(ctx))):
        return True
    if c['install_status'] == 'latest':
        updates = ctx.salt['pkg.list_upgrades'](refresh=True)
        return any((p in updates for p in config.packages(ctx)))
    return False


def plan(ctx):
    c = ctx.config
    pending = (
        json.loads(journal.pending_path(ctx).read_text())
        if journal.pending_path(ctx).exists()
        else None
    )
    if pending:
        return dict(pending, needed=True)
    changed = journal.file_changed(
        ctx, ctx.path('work') + '/redpanda.yaml', ctx.path('config')
    )
    changed |= journal.file_changed(
        ctx, ctx.path('work') + '/systemd.conf', ctx.path('systemd_dropin')
    )
    if c['enable_tls']:
        changed |= any(
            (
                journal.file_changed(
                    ctx,
                    ctx.path('work') + '/' + f,
                    ctx.path('cert_directory') + '/' + f,
                )
                for f in ('node.crt', 'node.key', 'truststore.pem')
            )
        )
    if (
        c['enable_fips']
        and ctx.run(['/usr/bin/fips-mode-setup', '--check'])
        .strip()
        .lower()
        .find('disabled')
        >= 0
    ):
        raise RuntimeError('OS FIPS mode is disabled')
    old = config.initialized(ctx)
    node_id = None
    restart = False
    if old:
        if node_id is None:
            matches = [
                b
                for b in ctx.api('brokers')
                if b['internal_rpc_address'] == c['nodes'][ctx.minion_id]['private_ip']
            ]
            if len(matches) != 1:
                raise RuntimeError('Cannot uniquely identify broker by RPC address')
            node_id = matches[0]['node_id']
        restart = any(
            (
                s['restart']
                for s in ctx.api('cluster_config/status')
                if s['node_id'] == node_id
            )
        )
    return dict(
        initialized=old,
        node_id=node_id,
        needed=not old
        or changed
        or package_changes(ctx)
        or restart
        or journal.pending_path(ctx).exists(),
    )


def local_ready(ctx):
    status = ctx.api('status/ready', local=True)
    return isinstance(status, dict) and status.get('status') == 'ready'


def _pending(ctx, test=False):
    if journal.pending_path(ctx).exists():
        return json.loads(journal.pending_path(ctx).read_text())
    if test:
        desired_plan = plan(ctx)
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
    desired_plan = plan(ctx)
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

    return _step(ctx, 'restored', 'prepared', test, action)


def health_before_drain(ctx, test=False):

    def action(p):
        if _multi(ctx, p):
            ctx.wait(lambda: local_ready(ctx), 'existing local broker readiness')
            wait_healthy(ctx)
        return False

    return _step(ctx, 'healthy_before_drain', 'restored', test, action)


def maintenance_enable(ctx, test=False):

    def action(p):
        if not _multi(ctx, p):
            return False
        status = ctx.api(f"brokers/{p['node_id']}").get('maintenance_status', {})
        if status.get('draining'):
            return False
        wait_healthy(ctx)
        ctx.api(f"brokers/{p['node_id']}/maintenance", 'PUT')
        ctx.record('maintenance', 'enabled')
        return True

    return _step(ctx, 'maintenance', 'healthy_before_drain', test, action)


def maintenance_wait(ctx, test=False):

    def action(p):
        if _multi(ctx, p):

            def drained():
                status = ctx.api(f"brokers/{p['node_id']}")['maintenance_status']
                if status.get('errors') or status.get('failed'):
                    raise ValueError('Maintenance drain failed')
                return status.get('finished', False)

            ctx.wait(drained, 'maintenance drain')
        return False

    return _step(ctx, 'drained', 'maintenance', test, action)


def safety_after_drain(ctx, test=False):

    def action(p):
        if _multi(ctx, p):
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

    return _step(ctx, 'safe_to_update', 'drained', test, action)


def service_mask(ctx, test=False):

    def action(p):
        ctx.run(['systemctl', 'mask', '--runtime', 'redpanda.service'])
        ctx.record('runtime_mask', True)
        return True

    return _step(ctx, 'masked', 'safe_to_update', test, action)


def safety_before_restart(ctx, test=False):

    def action(p):
        if ctx.salt['service.masked']('redpanda', runtime=True):
            raise RuntimeError(
                'Broker is still runtime masked; unmask must succeed before restart'
            )
        if _multi(ctx, p) and ctx.salt['service.status']('redpanda'):
            restart_safety(ctx, p['node_id'])
        return False

    return _step(ctx, 'safe_to_restart', 'masked', test, action)


def broker_start(ctx, test=False):

    def action(p):
        running = ctx.salt['service.status']('redpanda')
        if 'started' in p['steps'] and running:
            return False
        if _multi(ctx, p) and running:
            restart_safety(ctx, p['node_id'])
        fun = 'service.restart' if p['initialized'] and running else 'service.start'
        if not ctx.salt[fun]('redpanda'):
            raise RuntimeError('Failed to start/restart redpanda')
        ctx.record('service', fun.split('.')[-1])
        return True

    return _step(ctx, 'started', 'safe_to_restart', test, action)


def readiness_wait(ctx, test=False):

    def action(p):
        ctx.wait(lambda: local_ready(ctx), 'local broker readiness')
        return False

    return _step(ctx, 'ready', 'started', test, action)


def maintenance_disable(ctx, test=False):

    def action(p):
        if not _multi(ctx, p):
            return False
        status = ctx.api(f"brokers/{p['node_id']}").get('maintenance_status', {})
        if not status.get('draining'):
            return False
        ctx.wait(lambda: local_ready(ctx), 'local readiness before leaving maintenance')
        ctx.api(f"brokers/{p['node_id']}/maintenance", 'DELETE')
        ctx.record('maintenance', 'disabled')
        return True

    return _step(ctx, 'maintenance_cleared', 'ready', test, action)


def health_after_start(ctx, test=False):

    def action(p):
        if p['initialized']:
            wait_healthy(ctx)
        return False

    return _step(ctx, 'healthy_after_start', 'maintenance_cleared', test, action)


def transaction_complete(ctx, test=False):
    pending = _pending(ctx, test)
    if pending is None:
        return {'changed': False}
    if test:
        return {'changed': True}
    if 'healthy_after_start' not in pending.get('steps', []):
        raise RuntimeError('Cannot complete transaction before final health check')
    ctx.wait(lambda: local_ready(ctx), 'readiness before completing transaction')
    if pending['initialized']:
        wait_healthy(ctx)
    Path(ctx.path('bootstrap_env')).unlink(missing_ok=True)
    journal.pending_path(ctx).unlink()
    ctx.record('pending', 'cleared')
    return {'changed': True}


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
        ctx.wait(lambda: local_ready(ctx), 'existing broker readiness')
    return dict(changed=True)
