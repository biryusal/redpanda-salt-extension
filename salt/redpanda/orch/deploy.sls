{% set rp = pillar.get('redpanda', {}) %}
{% set nodes = rp.get('nodes', {}) %}
{% if not nodes %}
redpanda-empty-inventory:
  test.fail_without_changes:
    - name: Pass redpanda.nodes in orchestration pillar
    - failhard: true
{% else %}
redpanda-sync:
  salt.function:
    - name: saltutil.sync_all
    - tgt: {{ nodes.keys() | list | tojson }}
    - tgt_type: list
    - failhard: true
redpanda-prepare:
  salt.state:
    - tgt: {{ nodes.keys() | list | tojson }}
    - tgt_type: list
    - sls: redpanda.redpanda_broker.prepare
    - pillar: {{ {'redpanda': rp} | tojson }}
    - require:
      - salt: redpanda-sync
    - failhard: true
redpanda-bootstrap:
  salt.state:
    - tgt: {{ nodes.keys() | list | tojson }}
    - tgt_type: list
    - sls: redpanda.redpanda_broker.bootstrap
    - pillar: {{ {'redpanda': rp} | tojson }}
    - require:
      - salt: redpanda-prepare
    - failhard: true
redpanda-restore-services:
  salt.state:
    - tgt: {{ nodes.keys() | list | tojson }}
    - tgt_type: list
    - sls: redpanda.redpanda_broker.restore
    - pillar: {{ {'redpanda': rp} | tojson }}
    - require:
      - salt: redpanda-bootstrap
    - failhard: true
{% for id in nodes %}
redpanda-recover-{{ loop.index }}:
  salt.state:
    - tgt: {{ [id] | tojson }}
    - tgt_type: list
    - sls: redpanda.redpanda_broker.recover
    - pillar: {{ {'redpanda': rp} | tojson }}
    - require:
      - salt: {{ 'redpanda-restore-services' if loop.first else 'redpanda-recover-' ~ (loop.index - 1) }}
    - failhard: true
{% endfor %}
redpanda-cluster:
  salt.state:
    - tgt: {{ [nodes.keys() | first] | tojson }}
    - tgt_type: list
    - sls: redpanda.redpanda_broker.cluster
    - pillar: {{ {'redpanda': rp} | tojson }}
    - require:
      - salt: redpanda-recover-{{ nodes | length }}
    - failhard: true
{% for id in nodes %}
redpanda-apply-{{ loop.index }}:
  salt.state:
    - tgt: {{ [id] | tojson }}
    - tgt_type: list
    - sls: redpanda.redpanda_broker.apply
    - pillar: {{ {'redpanda': rp} | tojson }}
    - require:
      - salt: {{ 'redpanda-cluster' if loop.first else 'redpanda-apply-' ~ (loop.index - 1) }}
    - failhard: true
{% endfor %}
{% endif %}
