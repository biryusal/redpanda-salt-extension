{% from 'redpanda/map.jinja' import rp with context %}
include:
  - redpanda.system_setup
redpanda-inotify:
  sysctl.present:
    - name: fs.inotify.max_user_instances
    - value: {{ rp.get('max_user_instances', 8192) }}
    - require:
      - module: redpanda-rollout-guard
redpanda-panic-on-oops:
  sysctl.present:
    - name: kernel.panic_on_oops
    - value: {{ rp.get('kernel_panic_on_oops', 1) }}
    - require:
      - module: redpanda-rollout-guard
