# Configuration

[Full pillar example](../pillar/redpanda.example.sls). Merge: defaults → pillar → nodes.<minion>.overrides → host_specific_override.

- Include all exact minion IDs; use identical cluster settings across brokers.
- Set identity, ports and data paths through formula settings; conflicting node overrides fail.
- present installs missing pinned packages; latest upgrades. Package versions must match all split packages; airgap needs each package source.
- Removed managed cluster properties reset to default; unmanaged ones remain. All brokers must acknowledge the config version.
- restart_node=false rejects changes needing restart. Select internal_kafka_listener when ambiguous.
- Production: TLS + SASL. Secrets come through protected pillar, not node.rpk. Admin credentials are passed to rpk via environment. Bootstrap secrets are removed after startup; component credentials remain in broker config.
- With SASL, Admin API auth defaults on; Proxy/Schema Registry default to http_basic/private bind. Explicitly preserve old behavior before separate auth/TLS migration.

**Storage:** explicit devices/UUID/mountpoint, with data inside that mount. For one device, resolve=true reads XFS UUID on the minion and preserves live paths/symlinks. initialize=true permits unused-device XFS creation; foreign signatures, partitions and UUID conflicts fail. Device ancestry is checked before mutation. Mount persists UUID in fstab. RAID uses explicit configuration.

**Users/ACLs:** separate users runner, current inventory/transport required. Omitted entries are retained; deletion needs state: absent. Password updates need update_password: true. SCRAM-SHA-256/512 and exact allow/deny, literal/prefixed Kafka ACLs are supported. Admin/superusers/active service accounts are protected; pending deployment blocks security changes. RBAC/Schema Registry ACLs are unsupported.

**Service accounts:** explicit accounts must differ from admin/superusers and each other. Rotate using a new username, complete rollout, then separately remove the old user. Existing credentials must match live credentials or the saved digest.
