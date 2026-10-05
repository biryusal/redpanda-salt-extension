# Redpanda Ansible Collection comparison

Reviewed revision `9744e957f5d431744eaa5ff309c71aeda2202e41`, collection 0.12.0.
This is a source review; upstream deployment was not reproduced.

| Upstream | This extension |
|---|---|
| `system_setup`, `sysctl_setup` | Native Salt OS resources |
| `redpanda_broker` | Bootstrap, packages/config, cluster properties, safe serial restart |
| `user_management` | Separate runner for SCRAM users and Kafka ACLs |
| Automatic disk discovery / optional SASL data clearing | Explicit devices; no automatic data deletion |
| Ansible include/set_fact and host loops | SLS dependencies and typed Python configuration |
| Console/Connect/demo CA/RBAC | Outside the broker extension's scope |

The upstream broker role creates service accounts during bootstrap, without password PUT.
A separate user_management role manages users on a running cluster, including explicit password updates;
the managing admin is protected. These are two management paths for the same SCRAM accounts.

In the reviewed source, client endpoints may differ from a custom Kafka listener port.
The upstream restart path lacks this extension's check that every broker has acknowledged the same config version.
This is not a claim about the results of a live upstream deployment.

Sources at the pinned revision:

- [Broker service accounts](https://github.com/redpanda-data/redpanda-ansible-collection/blob/9744e957f5d431744eaa5ff309c71aeda2202e41/roles/redpanda_broker/tasks/create-sasl-service-accounts.yml)
- [User management](https://github.com/redpanda-data/redpanda-ansible-collection/blob/9744e957f5d431744eaa5ff309c71aeda2202e41/roles/user_management/tasks/main.yml)
- [Restart/config](https://github.com/redpanda-data/redpanda-ansible-collection/blob/9744e957f5d431744eaa5ff309c71aeda2202e41/roles/redpanda_broker/tasks/start-redpanda.yml)
- [Listener defaults](https://github.com/redpanda-data/redpanda-ansible-collection/blob/9744e957f5d431744eaa5ff309c71aeda2202e41/roles/redpanda_broker/templates/configs/defaults.j2)
- [Internal client endpoints](https://github.com/redpanda-data/redpanda-ansible-collection/blob/9744e957f5d431744eaa5ff309c71aeda2202e41/roles/redpanda_broker/templates/configs/sasl.j2)
