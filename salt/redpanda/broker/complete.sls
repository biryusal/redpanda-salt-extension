# Check readiness and health before completing the transaction.
{% from 'redpanda/map.jinja' import rp with context %}
{% set initialized = salt['redpanda.initialized']() and not salt['redpanda.bootstrap_needed']() %}
redpanda-readiness:
  redpanda_broker.ready:
    - require:
      - redpanda_broker: redpanda-start
    - failhard: true
redpanda-maintenance-clear:
  redpanda_broker.maintenance_cleared:
    - require:
      - redpanda_broker: redpanda-readiness
    - failhard: true
redpanda-health-after-start:
  redpanda_broker.healthy_after_start:
    - require:
      - redpanda_broker: redpanda-maintenance-clear
    - failhard: true
redpanda-enable:
  service.enabled:
    - name: redpanda
    - require:
      - redpanda_broker: redpanda-health-after-start
    - failhard: true
redpanda-running:
  redpanda_broker.completed:
    - require:
      - service: redpanda-enable
    - failhard: true
