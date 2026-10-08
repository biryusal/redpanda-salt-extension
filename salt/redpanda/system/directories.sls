{% from 'redpanda/map.jinja' import rp with context %}
redpanda-work-directory:
  file.directory:
    - name: {{ rp.paths.work }}
    - user: root
    - group: root
    - mode: '0700'
    - require:
      - module: redpanda-validate
