# Validation coverage

Local command: `pytest -q`. Uses Python 3.12, Salt 3008.3, and pytest-salt-factories.
CI also specifies Salt 3007.8; that version has not been tested locally.
After the refactor: **184 passed**, including a separate `salt-call` with `sync_all`.
Two dependency warnings: Tornado event loop and deprecated `crypt`.

| Tests | Coverage |
|---|---|
| Unit | Desired/actual, no-op, dry-run, password rotation, ACL identity, partial failures |
| Render/graph | Debian/RedHat, TLS/SASL, storage/airgap/nightly/FIPS; correct dependencies |
| Native loader | Extension/package import and highstate compilation |
| Functional/CLI | Real Salt dispatch and sync_all/show_sls in a separate salt-call |

Compiler tests simulate Linux grains. HTTP/systemd/package effects are mocked.
VMs are created separately through Terraform; provisioning and a VM test runner are not included here.

## VM acceptance scenarios

| Scenario | Expected result |
|---|---|
| Bootstrap three brokers | Membership, produce/consume, and reboot work |
| Repeat deployment | No config/package drift, maintenance, or restart |
| Node config / packages / restart-required property | One broker updated at a time; readiness/health checked after each |
| Dynamic cluster property | Applied without unnecessary restarts |
| TLS/SASL/mTLS, service accounts | Bootstrap and Kafka clients work |
| Package/config/start failure | Subsequent nodes untouched; pending/reservations retained |
| All brokers stopped | First restore quorum using existing configs |
| Peer disappears after drain / RF=1 risks | Restart rejected |
| Concurrent runners / master stops | Reservations retained; overlap prevented |
| Users/ACLs, rotation, exact deletion, repeat | Access matches rules; broker process not restarted |
| Cluster property removed / propagation timeout | Default restored; retry waits for acknowledged version |
| Runtime mask, reload, oneshot tuner | Package hooks do not start an unprepared broker |
| Dedicated disk, missing volume, RAID0/XFS | Root/foreign disks protected; persistent volume blocks boot |
| Admin port / systemd drop-in | Updates and recovery use the correct endpoints |

Nightly/airgap/FIPS, license, and tiered storage are tested with the corresponding packages/credentials.
Mixed-version upgrades must follow a supported Redpanda upgrade path.
Packages/config are not rolled back automatically. A health check cannot rule out a new failure after the check.
