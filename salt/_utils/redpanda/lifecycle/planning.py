"""Compare current and desired state without applying broker changes."""

import json
from .. import config, journal


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
