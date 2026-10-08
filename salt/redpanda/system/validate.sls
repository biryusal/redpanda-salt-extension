{% from 'redpanda/map.jinja' import rp with context %}
redpanda-rollout-guard:
  module.run:
    - name: redpanda.require_rollout
    - failhard: true
redpanda-validate:
  module.run:
    - name: redpanda.validate
    - require:
      - module: redpanda-rollout-guard
    - failhard: true
