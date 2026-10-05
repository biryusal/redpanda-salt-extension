# Review results

Fixes:

- Service accounts cannot collide with admin/superusers; existing credentials are not overwritten.
- Cluster config checks propagation across all brokers, including no-op/retry; the pending version is persisted.
- Internal Kafka endpoints respect the selected listener and each node's port.
- Application users/ACLs moved out of deployment into a separate runner sharing the reservation mechanism.
- Unmask/reload/tuner use native Salt; safety/recovery remain explicitly checked operations.

Refactoring:

- The Salt adapter handles only call authorization, config resolution, and result formatting.
- Logic is split into `_utils/redpanda/`; dependencies are passed through `Context`.
- No hidden ContextVar or callbacks from the core into the execution module.
- SLS owns ordering; the journal owns recovery progress. Public Salt names and checkpoint formats are preserved.

[Architecture](architecture.md) · [Validation and limitations](acceptance.md) · [Ansible comparison](ansible-collection-audit.md)
