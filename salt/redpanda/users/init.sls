{% from 'redpanda/map.jinja' import rp with context %}
include:
  - redpanda.users.validate
redpanda-security-work-directory:
  file.directory:
    - name: {{ rp.paths.work | tojson }}
    - user: root
    - group: root
    - mode: '0700'
    - require:
      - module: redpanda-security-validate
    - failhard: true
redpanda-users:
  redpanda_broker.users_managed:
    - require:
      - file: redpanda-security-work-directory
    - failhard: true
redpanda-acls:
  redpanda_broker.acls_managed:
    - require:
      - redpanda_broker: redpanda-users
    - failhard: true
