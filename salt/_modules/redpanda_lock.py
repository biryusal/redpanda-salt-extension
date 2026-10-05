"""Persistent per-host rollout reservations. No expiry: recovery is explicit."""
import fcntl
import json
import os
from pathlib import Path

__virtualname__ = 'redpanda_lock'


def __virtual__():
    return __virtualname__


def _root():
    # Stable across pillar changes and clusters sharing the same broker host.
    root = Path(__opts__['cachedir']) / 'redpanda-rollout'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root


def _access(token, action):
    if not isinstance(token, str) or not token or len(token) > 128:
        raise ValueError('A runner-owned rollout_token is required')
    root = _root()
    # Never unlink this inode: otherwise two processes can hold distinct locks.
    fd = os.open(root / 'gate', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        path = root / 'reservation.json'
        current = json.loads(path.read_text()) if path.exists() else None
        if current and current['token'] != token:
            raise RuntimeError('Host reserved by another rollout')
        if action == 'acquire':
            if not current:
                tmp = root / 'reservation.tmp'
                tmp.write_text(json.dumps({'token': token}))
                tmp.chmod(0o600)
                tmp.replace(path)
        elif action == 'check':
            if not current:
                raise RuntimeError('Host has no rollout reservation; use redpanda_rollout.deploy')
        elif action == 'release':
            jobs = __salt__['saltutil.running']()
            if any(j.get('fun', '').startswith(('state.', 'redpanda.')) for j in jobs):
                raise RuntimeError('Broker/state jobs still running; reservation retained')
            path.unlink(missing_ok=True)
        return True
    finally:
        os.close(fd)


def acquire(token):
    """Reserve this host atomically; same token is idempotent."""
    return _access(token, 'acquire')


def check(token):
    """Validate ownership before changing broker or OS resources."""
    return _access(token, 'check')


def release(token):
    """Explicit unlock, only after all broker/state jobs on this host have stopped."""
    return _access(token, 'release')
