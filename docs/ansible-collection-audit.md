# Upstream audit

Source reviewed: redpanda.cluster 0.12.0, revision [9744e957](https://github.com/redpanda-data/redpanda-ansible-collection/tree/9744e957f5d431744eaa5ff309c71aeda2202e41). Upstream deployment was not reproduced.

Equivalent areas: OS/sysctl, broker packages/config, SCRAM users and Kafka ACLs. Extension adds explicit device selection, persistent reservations/journal and config-propagation checks; users/ACLs use a separate runner. Automatic data deletion is unsupported. Console/Connect/demo CA/RBAC are outside scope.

Source entry points: system_setup, redpanda_broker and user_management roles at the pinned revision. This audit documents implementation provenance, not live behavioral equivalence.
