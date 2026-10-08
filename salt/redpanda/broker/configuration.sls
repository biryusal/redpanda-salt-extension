# Copy staged configuration, bootstrap settings and TLS certificates.
{% from 'redpanda/map.jinja' import rp with context %}
{% set initialized = salt['redpanda.initialized']() and not salt['redpanda.bootstrap_needed']() %}
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
