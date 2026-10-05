{% set rp = pillar.get('redpanda', {}) %}
{% set nodes = rp.get('nodes', {}) %}
{% if not nodes %}
redpanda-empty-security-inventory:
  test.fail_without_changes:
    - name: Pass the complete redpanda.nodes inventory
    - failhard: true
{% else %}
redpanda-security-sync:
  salt.function:
    - name: saltutil.sync_all
    - tgt: {{ nodes.keys() | list | tojson }}
    - tgt_type: list
    - failhard: true
redpanda-security-preflight:
  salt.state:
    - tgt: {{ nodes.keys() | list | tojson }}
    - tgt_type: list
    - sls: redpanda.users.validate
    - pillar: {{ {'redpanda': rp} | tojson }}
    - require:
      - salt: redpanda-security-sync
    - failhard: true
redpanda-security-apply:
  salt.state:
    - tgt: {{ [nodes.keys() | first] | tojson }}
    - tgt_type: list
    - sls: redpanda.users
    - pillar: {{ {'redpanda': rp} | tojson }}
    - require:
      - salt: redpanda-security-preflight
    - failhard: true
{% endif %}
