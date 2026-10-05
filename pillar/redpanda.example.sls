# Copy into your pillar root and include in pillar/top.sls.
# Pass the same cluster pillar to orchestration; nodes are Salt minion IDs.
redpanda:
  version: '25.3.1'  # Example only: select your release and exact package version.
  install_status: present
  nodes:
    broker-1:
      private_ip: 10.0.0.11
      rack: zone-a
      overrides:
        tls:
          cert_source: salt://private/redpanda/broker-1.crt
          key_source: salt://private/redpanda/broker-1.key
    broker-2:
      private_ip: 10.0.0.12
      rack: zone-b
      overrides:
        tls:
          cert_source: salt://private/redpanda/broker-2.crt
          key_source: salt://private/redpanda/broker-2.key
    broker-3:
      private_ip: 10.0.0.13
      rack: zone-c
      overrides:
        tls:
          cert_source: salt://private/redpanda/broker-3.crt
          key_source: salt://private/redpanda/broker-3.key
  data_directory: /var/lib/redpanda/data
  storage:
    devices: []  # Existing filesystem by default; no automatic NVMe discovery.
    format: false
  restart_node: true
  timeout: 300
  enable_tls: true
  kafka_enable_authorization: true
  tls:
    ca_source: salt://private/redpanda/truststore.pem
    require_client_auth: false
  sasl:
    username: admin
    password: <supply through encrypted pillar>
  sasl_users: []
  sasl_acls: []
  node: {}
  cluster: {}
  host_specific_override: {}
  # sasl_users:
  #   - username: app
  #     password: <supply through encrypted pillar>
  #     mechanism: SCRAM-SHA-512
  #     state: present
  #     update_password: true
  #   - username: retired-app
  #     state: absent
  # sasl_acls:
  #   - username: app
  #     resource_type: topic
  #     resource_name: orders
  #     operation: read
  #     permission: allow
  #     pattern: literal
  #     host: '*'
  #     state: present
  # Apply sasl_users/sasl_acls separately with redpanda_rollout.users.
  # cluster:
  #   default_topic_replications: 3
