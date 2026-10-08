# Unmask, reload, tune and safely start the broker.
{% from 'redpanda/map.jinja' import rp with context %}
{% set initialized = salt['redpanda.initialized']() and not salt['redpanda.bootstrap_needed']() %}
redpanda-unmask:
  service.unmasked:
    - name: redpanda
    - runtime: true
    - onlyif: test -f {{ (rp.paths.work + '/pending.json') | quote }}
    - require:
      - file: redpanda-node-config
      - file: redpanda-systemd-bootstrap
{% if not initialized %}
      - file: redpanda-bootstrap-config
{% if rp.kafka_enable_authorization %}
      - file: redpanda-bootstrap-user
{% endif %}
{% endif %}
{% if rp.enable_tls %}
{% for filename in ['truststore.pem', 'node.crt', 'node.key'] %}
      - file: redpanda-cert-{{ filename }}
{% endfor %}
{% endif %}
    - failhard: true
redpanda-systemd-reload:
  module.run:
    - name: service.systemctl_reload
    - onlyif: test -f {{ (rp.paths.work + '/pending.json') | quote }}
    - require:
      - service: redpanda-unmask
    - failhard: true
redpanda-tuner:
  module.run:
    - name: service.start
    - m_name: redpanda-tuner
    - onlyif: test -f {{ (rp.paths.work + '/pending.json') | quote }}
    - require:
      - module: redpanda-systemd-reload
    - failhard: true
redpanda-safety-before-restart:
  redpanda_broker.safe_before_restart:
    - require:
      - module: redpanda-tuner
    - failhard: true
redpanda-start:
  redpanda_broker.started:
    - require:
      - redpanda_broker: redpanda-safety-before-restart
    - failhard: true
