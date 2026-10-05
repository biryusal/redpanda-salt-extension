# Configuration rules

See the [pillar example](../pillar/redpanda.example.sls) for settings. Defaults: `salt/redpanda/defaults.json`.

| Area | Rule |
|---|---|
| Inventory | Include all brokers; use the same inventory for both runners |
| Merge | Defaults → pillar → `nodes.<minion>.overrides`; `host_specific_override` finalizes node/cluster YAML |
| Identity | Set `data_directory`, RPC/admin ports, and nodes through formula settings; conflicting YAML overrides are rejected |
| Cluster | Use identical settings on all nodes; apply once on the first node |
| Ownership | Removing a previously managed cluster property resets it to its default; unmanaged properties are retained |
| Propagation | All brokers must acknowledge one config version without invalid/unknown properties; checked even on no-op |
| Latest | `install_status: present` installs missing packages; `latest` also upgrades |
| Restart | `restart_node: false` rejects changes requiring restart before files/packages are replaced |
| Kafka | `internal_kafka_listener`: `internal` or the only listener; otherwise an explicit selection is required |
| Endpoints | The internal listener binds to the private IP or a wildcard; client ports respect each broker's overrides |
| Storage | Explicit devices and UUID only; data directory must be inside the mountpoint; formatting requires separate opt-in |
| Packages | A pinned version must be the exact distribution version for all split packages; airgap requires a source for each package |
| Paths | Changing service/config paths requires matching systemd configuration |

## Users and ACLs

```yaml
redpanda:
  # Include the complete nodes inventory and current TLS/listener settings.
  kafka_enable_authorization: true
  enable_tls: true
  sasl: {username: admin, password: '<from protected pillar>'}
  sasl_users:
    - {username: app, password: '<from protected pillar>', update_password: true}
    - {username: retired, state: absent}
  sasl_acls:
    - {username: app, resource_type: topic, resource_name: orders, operation: read}
```

- User defaults: `present`, mechanism `SCRAM-SHA-256`; `SCRAM-SHA-512` is also supported.
- Passwords change only with `update_password: true`; an unchanged digest on a subsequent run does not trigger PUT.
- Deletion requires `state: absent`; users/ACLs removed from pillar are retained.
- ACL: one exact rule; defaults are `allow`, host `*`, pattern `literal`, state `present`.
- Resources: `topic`, `group`, `transactional_id`, `cluster` (`kafka-cluster`, literal).
- Operations: `all`, `read`, `write`, `create`, `delete`, `alter`, `describe`,
  `describe_configs`, `alter_configs`, `cluster_action`, `idempotent_write`.
- `deny`, `prefixed`, and explicit `absent` are supported; ambiguous lookups stop mutation.
- All declared superusers (including cluster/host overrides), the admin, and active service accounts are protected. RBAC and Schema Registry ACLs are not implemented.
- Pending broker deployment blocks the security runner; ACL management requires installed rpk.

## Internal service accounts

These are SCRAM users for Schema Registry/Pandaproxy, not a separate account type.
With `sasl_use_explicit_service_accounts`, names must differ from each other and from admin/superusers.
Change a password using a **new username**: deployment creates the account before switching brokers.
After the full rollout, the old user can be explicitly deleted in a separate security run.
An existing account is accepted only when live credentials or the saved digest match.

## Security and migration from older configuration

Production pillar should enable TLS and Kafka authorization; the example enables both.
Low-level defaults retain opt-in TLS/SASL for existing installations.
SASL without TLS is prohibited unless `allow_insecure_sasl: true` is explicitly set for an isolated test environment.
With SASL, Admin API authentication is required by default; Proxy and Schema Registry use
`http_basic` and bind to the private IP. Specify the previous behavior explicitly using
`admin_api_require_auth: false`, `pandaproxy_authn_method: none`,
`schema_registry_authn_method: none`; configure wildcard binds through node overrides.

Deployment removes admin credentials from the generated `rpk` config. Salt rpk commands receive
them through environment variables from pillar; manual rpk calls require RPK_USER/RPK_PASS or a separate
protected operator config. Credentials in `node.rpk` overrides are rejected.
The bootstrap environment file is removed after successful startup; service-account credentials
remain in broker config because the components need them.
Changes to defaults require HTTP clients to be updated before rollout: authentication and private-IP binds
may break existing connections. First update an existing cluster with its previous settings explicitly specified,
then perform a separate TLS/authentication migration.
