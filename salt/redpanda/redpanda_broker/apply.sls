{% from 'redpanda/map.jinja' import rp with context %}
{% set initialized = salt['redpanda.initialized']() and not salt['redpanda.bootstrap_needed']() %}
include:
  - redpanda.redpanda_broker.prepare
redpanda-restore-service:
  redpanda_broker.restored:
    - require:
      - sls: redpanda.redpanda_broker.prepare
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
redpanda-packages:
  pkg.{{ 'latest' if rp.version == 'latest' and rp.install_status == 'latest' else 'installed' }}:
    - onlyif: test -f {{ (rp.paths.work + '/pending.json') | quote }}
{% if rp.airgap %}
    - sources: {{ rp.airgap_sources | tojson }}
{% else %}
    - pkgs:
{% for package in salt['redpanda.packages']() %}
      - {{ package }}{% if rp.version != 'latest' %}: {{ rp.version | tojson }}{% endif %}
{% endfor %}
    - refresh: true
{% endif %}
    - require:
      - redpanda_broker: redpanda-mask
{% if not rp.airgap %}
      - pkgrepo: redpanda-repository
{% endif %}
    - failhard: true
redpanda-package-version:
  module.run:
    - name: redpanda.validate_packages
    - require:
      - pkg: redpanda-packages
    - failhard: true
redpanda-config-directory:
  file.directory:
    - name: {{ rp.paths.config_directory | tojson }}
    - user: {{ rp.user | tojson }}
    - group: {{ rp.group | tojson }}
    - mode: '0750'
    - require:
      - module: redpanda-package-version
redpanda-node-config:
  file.managed:
    - name: {{ rp.paths.config }}
    - source: {{ rp.paths.work }}/redpanda.yaml
    - user: {{ rp.user | tojson }}
    - group: {{ rp.group | tojson }}
    - mode: '0600'
    - show_changes: false
    - require:
      - file: redpanda-config-directory
{% if not initialized %}
redpanda-bootstrap-config:
  file.managed:
    - name: {{ rp.paths.bootstrap_config | tojson }}
    - source: {{ rp.paths.work }}/bootstrap.yaml
    - user: {{ rp.user | tojson }}
    - group: {{ rp.group | tojson }}
    - mode: '0600'
    - show_changes: false
    - require:
      - file: redpanda-config-directory
{% if rp.kafka_enable_authorization %}
redpanda-bootstrap-user:
  file.managed:
    - name: {{ rp.paths.bootstrap_env }}
    - contents: {{ salt['redpanda.bootstrap_environment']() | tojson }}
    - mode: '0600'
    - makedirs: true
    - show_changes: false
    - require:
      - module: redpanda-package-version
{% endif %}
{% endif %}
redpanda-systemd-bootstrap:
  file.managed:
    - name: {{ rp.paths.systemd_dropin }}
    - source: {{ rp.paths.work }}/systemd.conf
    - makedirs: true
    - mode: '0644'
    - require:
      - module: redpanda-package-version
{% if rp.enable_tls %}
{% for filename in ['truststore.pem', 'node.crt', 'node.key'] %}
redpanda-cert-{{ filename }}:
  file.managed:
    - name: {{ (rp.paths.cert_directory + '/' + filename) | tojson }}
    - source: {{ (rp.paths.work + '/' + filename) | tojson }}
    - user: {{ rp.user | tojson }}
    - group: {{ rp.group | tojson }}
    - mode: '0600'
    - makedirs: true
    - show_changes: false
    - require:
      - file: redpanda-config-directory
{% endfor %}
{% endif %}
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
