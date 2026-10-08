# Architecture

Start with the deployment sequence, then follow one node down to its Python operation.

```text
pillar/redpanda.example.sls + salt/redpanda/defaults.json
  → salt/_runners/redpanda_rollout.py: deploy()
  → salt/redpanda/orch/deploy.sls
  → salt/redpanda/broker/apply.sls
  → salt/redpanda/broker/<step>.sls
  → salt/_states/redpanda_broker.py
  → salt/_modules/redpanda.py
  → salt/_utils/redpanda/<area>/<operation>.py
```

## Deployment sequence

The runner reserves every broker before starting orchestration. `orch/deploy.sls`
syncs extensions and runs these phases:

1. `broker/prepare.sls`: prepare the OS, repository and staged configuration.
2. `broker/bootstrap.sls`: apply the broker lifecycle to new nodes.
3. `broker/restore.sls`: restore existing stopped services when no transaction is pending.
4. `broker/recover.sls`: recover initialized nodes with pending transactions, one at a time.
5. `broker/cluster.sls`: reconcile license, cluster settings and service accounts.
6. `broker/apply.sls`: update brokers one at a time; unchanged running brokers do not restart.

Reservations are released after success. Failures retain reservations and partial progress.
`users()` runs `orch/users.sls` instead: validate every node, then reconcile application
users and ACLs on one node. It uses the same reservations as deployment.

## One broker update

`broker/apply.sls` is the table of contents for the update:

| SLS file | Responsibility | Python implementation |
|---|---|---|
| `prepare.sls` | Host preparation and staging | `config/`, `storage.py` |
| `drain.sls` | Restore, open transaction, check health, drain and mask | `lifecycle/transaction.py`, `health.py`, `maintenance.py`, `service.py` |
| `packages.sls` | Install and verify broker packages | `config/package.py` |
| `configuration.sls` | Copy staged config, bootstrap files and certificates | Native Salt file states |
| `service.sls` | Unmask, reload systemd, tune and safely start | `lifecycle/service.py`, `health.py` |
| `complete.sls` | Check readiness, leave maintenance, check health, close transaction | `lifecycle/health.py`, `maintenance.py`, `transaction.py` |

Every boundary keeps explicit `require` edges. `include` lists files; it does not
guarantee execution order. The graph tests and native Salt compiler tests verify the order.

## Where to change code

| Area | Files |
|---|---|
| OS preparation | `redpanda/system/`: validation, packages, user, directories, storage, proxy, sysctl |
| Configuration layers | `_utils/redpanda/config/layers.py` |
| Broker/cluster config generation | `config/render.py` |
| Configuration validation | `config/validation.py` |
| Package selection and version checks | `config/package.py` |
| Initialization detection and validated configuration | `config/node.py` |
| Change detection | `lifecycle/planning.py` |
| Checkpoints and transaction completion | `lifecycle/transaction.py`, `journal.py` |
| Readiness and restart safety | `lifecycle/health.py` |
| Drain and service operations | `lifecycle/maintenance.py`, `service.py` |
| Security preflight and protected identities | `security/validate.py` |
| Application users, ACLs, internal accounts | `security/users.py`, `acls.py`, `accounts.py` |
| Cluster configuration and license | `cluster.py` |
| External I/O | `admin.py`, `rpk.py`, `context.py` |
| Reservations | `_runners/redpanda_rollout.py`, `_modules/redpanda_lock.py` |

The `_states` adapter formats Salt results. The `_modules` adapter resolves pillar,
checks reservation ownership and dispatches operations. Domain packages expose their
public functions through `__init__.py`; implementation files contain the actual logic.

Native Salt states manage packages, files and systemd. Python compares actual and
desired state, calls Redpanda APIs and rechecks safety before restart. Dependencies
are passed through `Context`. The journal records progress, not health.

## Checks

Run `pytest -q` with `requirements-test.txt` installed. Tests cover Python operations,
SLS rendering, dependency order, native Salt loading/compilation and state execution
with mocked external I/O. CI runs the suite with Salt 3007 and 3008.
