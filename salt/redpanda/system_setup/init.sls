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
redpanda-dependencies:
  pkg.installed:
    - pkgs: {{ rp.platforms[grains.os_family].dependencies | tojson }}
    - require:
      - module: redpanda-validate
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
redpanda-work-directory:
  file.directory:
    - name: {{ rp.paths.work }}
    - user: root
    - group: root
    - mode: '0700'
    - require:
      - module: redpanda-validate
include:
  - redpanda.system_setup.storage
  - redpanda.system_setup.proxy
