"""Salt states for cluster-aware broker lifecycle (Apache-2.0)."""
__virtualname__ = 'redpanda_broker'


def __virtual__():
    return __virtualname__ if 'redpanda.transaction_prepare' in __salt__ else (False, 'Sync the redpanda execution module first')


def _call(name, function, config=None):
    ret = dict(name=name, result=True, changes={}, comment='Already in the desired state')
    try:
        kwargs = {'test': __opts__.get('test', False)}
        if config is not None:
            kwargs['config'] = config
        value = __salt__['redpanda.' + function](**kwargs)
        changed = value.get('needed', value.get('changed', False))
        if changed:
            ret['result'] = None if __opts__.get('test') else True
            ret['changes'] = {} if __opts__.get('test') else value.get('changes', {function: {k: v for k, v in value.items() if k not in ('needed', 'changed', 'admin_addresses')}})
            ret['comment'] = 'Changes required' if __opts__.get('test') else 'Changes applied'
    except Exception as exc:
        ret.update(result=False, comment=str(exc), changes=getattr(exc, 'changes', {}))
    return ret


def transaction(name, config=None):
    return _call(name, 'transaction_prepare', config)

def pending_restored(name, config=None):
    return _call(name, 'pending_restore', config)

def healthy_before_drain(name, config=None):
    return _call(name, 'health_before_drain', config)

def maintenance(name, config=None):
    return _call(name, 'maintenance_enable', config)

def drained(name, config=None):
    return _call(name, 'maintenance_wait', config)

def safe_after_drain(name, config=None):
    return _call(name, 'safety_after_drain', config)

def masked(name, config=None):
    return _call(name, 'service_mask', config)

def safe_before_restart(name, config=None):
    return _call(name, 'safety_before_restart', config)

def started(name, config=None):
    return _call(name, 'broker_start', config)

def ready(name, config=None):
    return _call(name, 'readiness_wait', config)

def maintenance_cleared(name, config=None):
    return _call(name, 'maintenance_disable', config)

def healthy_after_start(name, config=None):
    return _call(name, 'health_after_start', config)

def completed(name, config=None):
    return _call(name, 'transaction_complete', config)


def cluster_configured(name, config=None):
    return _call(name, 'cluster_config', config)


def licensed(name, config=None):
    return _call(name, 'license_present', config)


def service_accounts_present(name, config=None):
    return _call(name, 'service_accounts', config)


def users_managed(name, config=None):
    return _call(name, 'users_managed', config)


def acls_managed(name, config=None):
    return _call(name, 'acls_managed', config)


def restored(name, config=None):
    return _call(name, 'restore_service', config)
