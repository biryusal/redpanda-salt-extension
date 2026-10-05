"""Explicit settings and I/O dependencies for a single operation."""

import time
from . import admin, rpk


class Context:
    def __init__(self, config, inventory, minion_id, grains, salt):
        self.config = config
        self.inventory = inventory
        self.minion_id = minion_id
        self.grains = grains
        self.salt = salt
        self.changes = {}
        self.open_request = admin.open_request

    def path(self, name):
        return self.config['paths'][name]

    def api(self, path, method='GET', data=None, local=False, raw=False):
        return admin.request(
            self.config,
            self.minion_id,
            admin.ports(self),
            self.open_request,
            path,
            method,
            data,
            local,
            raw,
        )

    def run(self, argv, env=None):
        if argv and argv[0] == 'rpk' and self.config['kafka_enable_authorization']:
            from .security import validate_transport

            validate_transport(self.config)
            env = dict(
                {'RPK_USER': self.config['sasl'].get('username', 'admin'),
                 'RPK_PASS': self.config['sasl']['password']},
                **(env or {}),
            )
        return rpk.run(self.salt, argv, env)

    def record(self, key, value):
        self.changes[key] = value

    def wait(self, predicate, description):
        deadline = time.monotonic() + self.config['timeout']
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return True
            except RuntimeError:
                pass
            time.sleep(2)
        raise RuntimeError('Timed out waiting for ' + description)
