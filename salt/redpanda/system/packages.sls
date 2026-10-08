{% from 'redpanda/map.jinja' import rp with context %}
redpanda-dependencies:
  pkg.installed:
    - pkgs: {{ rp.platforms[grains.os_family].dependencies | tojson }}
    - require:
      - module: redpanda-validate
