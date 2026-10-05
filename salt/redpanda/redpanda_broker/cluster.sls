redpanda-cluster-ready:
  module.run:
    - name: redpanda.wait_healthy
    - failhard: true
redpanda-license:
  redpanda_broker.licensed:
    - require:
      - module: redpanda-cluster-ready
    - failhard: true
redpanda-cluster-config:
  redpanda_broker.cluster_configured:
    - require:
      - redpanda_broker: redpanda-license
    - failhard: true
redpanda-service-accounts:
  redpanda_broker.service_accounts_present:
    - require:
      - redpanda_broker: redpanda-cluster-config
    - failhard: true
