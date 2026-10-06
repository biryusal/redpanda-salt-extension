"""Master-owned rollout entry point. Persistent minion leases fail closed."""
import copy
import fcntl
import os
from pathlib import Path
import uuid


def _hosts(nodes, fun, **kwargs):
    result = __salt__['salt.execute'](nodes, fun, tgt_type='list', kwarg=kwargs)
    if not isinstance(result, dict) or set(result) != set(nodes):
        raise RuntimeError('Incomplete minion response: reservations retained')
    if any(value is not True for value in result.values()):
        raise RuntimeError('Minion operation failed: reservations retained')
    return result


def _master_lock():
    root = Path(__opts__['cachedir'])
    root.mkdir(parents=True, exist_ok=True)
    fd = os.open(root / 'redpanda-rollout.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        raise RuntimeError('Another Redpanda rollout is running on this master') from None
    return fd


def _success(value):
    # Salt orchestrate uses a compound {data, outputter, retcode} result.
    if not isinstance(value, dict) or value.get('retcode', 0) != 0:
        return False
    data = value.get('data', value)
    found = []
    def visit(item):
        if isinstance(item, dict):
            if 'result' in item:
                found.append(item['result'])
            for child in item.values():
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)
    visit(data)
    return bool(found) and all(v is True for v in found)


def deploy(pillar=None, test=False):
    """Deploy broker core; pillar={'redpanda': {...}}. Returns token and summary.

    Any failure retains all reservations: verify pending master/minion jobs,
    then call unlock with the reported token and the original node inventory.
    No timeout expiry can admit a second rollout while a remote job continues.
    """
    return _run('redpanda.orch.deploy', pillar, test)


def users(pillar=None, test=False):
    """Manage application SCRAM users/ACLs without a broker deployment.

    Reserve every broker to serialize with deployment, validate on all hosts,
    then apply security changes once. Failure retains reservations for unlock.
    test=True validates inventory only, as in deploy.
    """
    return _run('redpanda.orch.users', pillar, test)


def _failures(outcome):
    """Keep actionable state errors, without copying every nested return."""
    failures = []
    def visit(value, path=()):
        if isinstance(value, dict):
            if value.get('result') is False:
                failures.append({
                    'path': '/'.join(path),
                    'state': value.get('__id__', value.get('name', '')),
                    'comment': value.get('comment', ''),
                    'jid': value.get('__jid__', ''),
                })
            for key, child in value.items():
                visit(child, path + (str(key),))
        elif isinstance(value, list):
            for child in value:
                visit(child, path)
    visit(outcome)
    return failures


def _run(sls, pillar, test):
    data = copy.deepcopy(pillar or {})
    rp = data.get('redpanda', {})
    nodes = list(rp.get('nodes', {}))
    if not nodes:
        raise ValueError('Pass redpanda.nodes in runner pillar')
    fd = _master_lock()
    token = uuid.uuid4().hex
    try:
        if test:
            return {'result': True, 'changes': {}, 'nodes': nodes,
                    'comment': 'Read-only inventory validation; no rollout executed'}
        sync = __salt__['salt.execute'](nodes, 'saltutil.sync_all', tgt_type='list')
        if not isinstance(sync, dict) or set(sync) != set(nodes) or any(not isinstance(v, dict) for v in sync.values()):
            raise RuntimeError('Cannot sync every broker minion')
        _hosts(nodes, 'redpanda_lock.acquire', token=token)
        rp['rollout_token'] = token
        outcome = __salt__['state.orchestrate'](sls, pillar=data)
        if not _success(outcome):
            __context__['retcode'] = 1
            return {'result': False, 'token': token, 'reservations': 'retained',
                    'comment': 'Rollout failed; inspect failures below',
                    'failures': _failures(outcome), 'retcode': outcome.get('retcode', 1) if isinstance(outcome, dict) else 1}
        _hosts(nodes, 'redpanda_lock.release', token=token)
        return {'result': True, 'token': token, 'reservations': 'released',
                'comment': 'Rollout completed', 'nodes': nodes}
    except Exception as exc:
        __context__['retcode'] = 1
        return {'result': False, 'token': token, 'nodes': nodes,
                'reservations': 'retained', 'comment': str(exc)}
    finally:
        os.close(fd)


def unlock(nodes, token):
    """Operator recovery after verifying no rollout remains queued/running.

    Cancelling queued master jobs is an operator prerequisite. A minion cannot
    detect jobs not yet delivered across a network partition.
    """
    fd = _master_lock()
    try:
        _hosts(list(nodes), 'redpanda_lock.release', token=token)
        return {'result': True, 'reservations': 'released'}
    finally:
        os.close(fd)
