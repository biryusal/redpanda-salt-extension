"""Salt boundary: resolve pillar, enforce reservation, report safe changes.

Broker logic lives in _utils/redpanda; this module contains no reconciliation.
"""

import json

__virtualname__ = 'redpanda'


def __virtual__():
    return __virtualname__


def _context(config=None):
    source = __salt__['cp.get_file_str']('salt://redpanda/defaults.json')
    if not source:
        raise RuntimeError('Cannot load redpanda/defaults.json from fileserver')
    core = _library()
    supplied = __pillar__.get('redpanda', {}) if config is None else config
    inventory = core.config.merge(json.loads(source), supplied)
    resolved = core.config.merge(
        inventory, inventory['nodes'].get(__grains__['id'], {}).get('overrides', {})
    )
    salt = __salt__.value() if hasattr(__salt__, 'value') else __salt__
    if resolved['storage'].get('resolve'):
        resolved = core.config.merge(resolved, core.storage.discover(resolved, salt['cmd.run_all']))
    return core.new_context(resolved, inventory, __grains__['id'], dict(__grains__), salt)


def _library():
    return __utils__['redpanda.library']()


def settings(config=None):
    return _context(config).config


def require_rollout(config=None):
    return __salt__['redpanda_lock.check'](settings(config).get('rollout_token'))


def _apply(function, test, config):
    ctx = _context(config)
    try:
        if not test:
            __salt__['redpanda_lock.check'](ctx.config.get('rollout_token'))
        result = function(ctx, test=test)
        if ctx.changes:
            result['changes'] = ctx.changes
        return result
    except Exception as exc:
        exc.changes = ctx.changes.copy()
        raise


def validate():
    return _library().config.validate(_context())


def packages():
    return _library().config.packages(_context())


def configuration():
    return _library().config.configuration(_context())


def initialized():
    return _library().config.initialized(_context())


def healthy():
    return _library().lifecycle.healthy(_context())


def wait_healthy():
    return _library().lifecycle.wait_healthy(_context())


def package_changes():
    return _library().lifecycle.package_changes(_context())


def plan():
    return _library().lifecycle.plan(_context())


def bootstrap_needed():
    return _library().journal.bootstrap_needed(_context())


def recovery_needed():
    return _library().journal.recovery_needed(_context())


def bootstrap_environment():
    return _library().config.bootstrap_environment(_context())


def validate_storage():
    return _library().storage.validate_storage(_context())


def local_ready():
    return _library().lifecycle.local_ready(_context())


def validate_packages():
    return _library().config.validate_packages(_context())


def validate_storage_devices():
    return _library().storage.validate_storage_devices(_context())


def restart_safety(node_id):
    return _library().lifecycle.restart_safety(_context(), node_id)


def transaction_prepare(test=False, config=None):
    return _apply(_library().lifecycle.transaction_prepare, test, config)


def pending_restore(test=False, config=None):
    return _apply(_library().lifecycle.pending_restore, test, config)


def health_before_drain(test=False, config=None):
    return _apply(_library().lifecycle.health_before_drain, test, config)


def maintenance_enable(test=False, config=None):
    return _apply(_library().lifecycle.maintenance_enable, test, config)


def maintenance_wait(test=False, config=None):
    return _apply(_library().lifecycle.maintenance_wait, test, config)


def safety_after_drain(test=False, config=None):
    return _apply(_library().lifecycle.safety_after_drain, test, config)


def service_mask(test=False, config=None):
    return _apply(_library().lifecycle.service_mask, test, config)


def safety_before_restart(test=False, config=None):
    return _apply(_library().lifecycle.safety_before_restart, test, config)


def broker_start(test=False, config=None):
    return _apply(_library().lifecycle.broker_start, test, config)


def readiness_wait(test=False, config=None):
    return _apply(_library().lifecycle.readiness_wait, test, config)


def maintenance_disable(test=False, config=None):
    return _apply(_library().lifecycle.maintenance_disable, test, config)


def health_after_start(test=False, config=None):
    return _apply(_library().lifecycle.health_after_start, test, config)


def transaction_complete(test=False, config=None):
    return _apply(_library().lifecycle.transaction_complete, test, config)


def restore_service(test=False, config=None):
    return _apply(_library().lifecycle.restore_service, test, config)


def cluster_config(test=False, config=None):
    return _apply(_library().cluster.cluster_config, test, config)


def license_present(test=False, config=None):
    return _apply(_library().cluster.license_present, test, config)


def service_accounts(test=False, config=None):
    return _apply(_library().security.service_accounts, test, config)


def users_managed(test=False, config=None):
    return _apply(_library().security.users_managed, test, config)


def acls_managed(test=False, config=None):
    return _apply(_library().security.acls_managed, test, config)


def validate_security(test=False, config=None):
    return _apply(_library().security.validate_security, test, config)
