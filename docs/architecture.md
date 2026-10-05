# Architecture

Runner reserves inventory; SLS orders operations; Python compares actual/desired state and applies changes.

- `_runners/redpanda_rollout.py`: deploy/users, master lock, reservations, unlock.
- `redpanda/orch/`: cluster ordering; `redpanda_broker/`: per-node lifecycle.
- `_modules/redpanda.py`: pillar resolution and operation authorization.
- `_states/redpanda_broker.py`: Salt result formatting.
- `_modules/redpanda_lock.py`: persistent per-minion leases.
- `_utils/redpanda/`: config, storage, Admin API/rpk, security, cluster, lifecycle and journal; dependencies passed through Context.

SLS defines order; Python rechecks safety before restart. Journal records recovery progress, not health. Failures preserve partial changes and reservations.
