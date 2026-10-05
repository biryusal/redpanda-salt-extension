{% from 'redpanda/map.jinja' import rp with context %}
{% set cfg = salt['redpanda.configuration']() %}
redpanda-stage-node:
  file.managed:
    - name: {{ rp.paths.work }}/redpanda.yaml
    - contents: {{ cfg.node | tojson | tojson }}
    - mode: '0600'
    - show_changes: false
    - require:
      - file: redpanda-work-directory
redpanda-stage-cluster:
  file.managed:
    - name: {{ rp.paths.work }}/bootstrap.yaml
    - contents: {{ cfg.cluster | tojson | tojson }}
    - mode: '0600'
    - show_changes: false
    - require:
      - file: redpanda-work-directory
{% if rp.enable_tls %}
{% for filename, source in [('truststore.pem', rp.tls.ca_source), ('node.crt', rp.tls.cert_source), ('node.key', rp.tls.key_source)] %}
redpanda-stage-{{ filename }}:
  file.managed:
    - name: {{ (rp.paths.work + '/' + filename) | tojson }}
    - source: {{ source | tojson }}
{% if rp.tls.get('source_hashes', {}).get(filename) %}
    - source_hash: {{ rp.tls.source_hashes[filename] | tojson }}
{% endif %}
    - mode: '0600'
    - show_changes: false
    - require:
      - file: redpanda-work-directory
{% endfor %}
{% endif %}

redpanda-stage-systemd:
  file.managed:
    - name: {{ rp.paths.work }}/systemd.conf
    - contents: |
        [Service]
        User={{ rp.user }}
        Group={{ rp.group }}
        EnvironmentFile=-{{ rp.paths.bootstrap_env }}
        [Unit]
        RequiresMountsFor={{ rp.data_directory | tojson }}
    - mode: '0600'
    - require:
      - file: redpanda-work-directory
