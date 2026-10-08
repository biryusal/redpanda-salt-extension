{% from 'redpanda/map.jinja' import rp with context %}
redpanda-group:
  group.present:
    - name: {{ rp.group | tojson }}
    - system: true
    - require:
      - module: redpanda-validate
redpanda-user:
  user.present:
    - name: {{ rp.user | tojson }}
    - gid: {{ rp.group | tojson }}
    - system: true
    - shell: /usr/sbin/nologin
    - createhome: false
    - require:
      - group: redpanda-group
