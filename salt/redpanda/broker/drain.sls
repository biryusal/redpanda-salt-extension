# Restore and prepare the transaction, then drain and mask the broker.
{% from 'redpanda/map.jinja' import rp with context %}
{% set initialized = salt['redpanda.initialized']() and not salt['redpanda.bootstrap_needed']() %}
redpanda-restore-service:
  redpanda_broker.restored:
    - require:
      - sls: redpanda.broker.prepare
    - failhard: true
redpanda-transaction:
  redpanda_broker.transaction:
    - require:
      - redpanda_broker: redpanda-restore-service
    - failhard: true
redpanda-pending-restore:
  redpanda_broker.pending_restored:
    - require:
      - redpanda_broker: redpanda-transaction
    - failhard: true
redpanda-health-before-drain:
  redpanda_broker.healthy_before_drain:
    - require:
      - redpanda_broker: redpanda-pending-restore
    - failhard: true
redpanda-maintenance:
  redpanda_broker.maintenance:
    - require:
      - redpanda_broker: redpanda-health-before-drain
    - failhard: true
redpanda-drain:
  redpanda_broker.drained:
    - require:
      - redpanda_broker: redpanda-maintenance
    - failhard: true
redpanda-safety-after-drain:
  redpanda_broker.safe_after_drain:
    - require:
      - redpanda_broker: redpanda-drain
    - failhard: true
redpanda-mask:
  redpanda_broker.masked:
    - require:
      - redpanda_broker: redpanda-safety-after-drain
    - failhard: true
