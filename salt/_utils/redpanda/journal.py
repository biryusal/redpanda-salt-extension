"""Persistent local progress for retries and recovery."""

import json
from pathlib import Path
from . import config


def pending_path(ctx):
    return Path(ctx.path('work') + '/pending.json')


def write_pending(ctx, value):
    pending_path(ctx).parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = pending_path(ctx).with_suffix('.tmp')
    tmp.write_text(json.dumps(value))
    tmp.chmod(0o600)
    tmp.replace(pending_path(ctx))


def write_json(ctx, path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, sort_keys=True))
    temporary.chmod(0o600)
    temporary.replace(path)


def file_changed(ctx, source, dest):
    return (
        not Path(source).exists()
        or not Path(dest).exists()
        or Path(source).read_bytes() != Path(dest).read_bytes()
    )


def bootstrap_needed(ctx):
    if pending_path(ctx).exists():
        return not json.loads(pending_path(ctx).read_text())['initialized']
    return not config.initialized(ctx)


def recovery_needed(ctx):
    if pending_path(ctx).exists():
        return json.loads(pending_path(ctx).read_text())['initialized']
    return False
