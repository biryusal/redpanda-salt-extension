# Validation

`pytest -q`: unit logic, SLS/render graph, native loader and salt-call dispatch. OS/API/package effects are mocked; these checks do not establish live failover behavior.

VM acceptance must cover bootstrap/reboot, no-op, serial upgrade, TLS/SASL, config propagation, users/ACLs, persistent storage, interrupted rollout and explicit unlock/retry. Verify that failures stop later brokers and root/foreign disks remain untouched.

Test nightly/airgap/FIPS with actual packages/credentials. Follow supported Redpanda upgrade paths. No automatic package/config rollback; health checks cannot prevent failures occurring afterward. VM provisioning is outside this repository.
