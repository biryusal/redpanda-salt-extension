# Redpanda Salt extension

Linux/systemd, Salt 3007/3008, Debian/Ubuntu or RedHat family. VMs and Salt are provisioned separately.

Serve `salt/` through file_roots or GitFS. Assign the [example pillar](pillar/redpanda.example.sls) to exact minion IDs; include every broker, exact package version and protected SASL/TLS settings.

```sh
salt-run saltutil.sync_runners
salt-run redpanda_rollout.deploy pillar="$(cat cluster-pillar.json)"
salt-run redpanda_rollout.users pillar="$(cat cluster-pillar.json)"
```

`cluster-pillar.json` contains `{"redpanda": {...}}`. Run pillar is visible to authorized Salt operators/job cache. Direct state.apply/highstate is unsupported; use the runner.

Deployment: reserve all nodes → prepare/bootstrap/recover → reconcile cluster → update existing brokers one at a time → release after success. Unchanged running brokers are not restarted. Users/ACLs are a separate workflow.

Failure retains reservations and pending.json. Stop queued/running jobs, fix cause, unlock with the reported token and original node list, retry. Never delete the journal.

Disks must be explicit; initialization requires opt-in. Existing XFS/data are preserved. SASL requires TLS unless explicitly overridden for an isolated test. TLS/admin/membership migrations are separate operations. Restart probes require Redpanda 25.1+; RF=1 risks block restarts by default. Runner test=True validates inventory only.

[Configuration](docs/configuration.md) · [Architecture](docs/architecture.md) · [Validation](docs/acceptance.md) · [Upstream audit](docs/ansible-collection-audit.md).

Tests: install `requirements-test.txt`, run `pytest -q`. Apache-2.0: [LICENSE](LICENSE), [NOTICE](NOTICE).
