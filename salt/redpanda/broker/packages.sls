# Install packages only while a broker transaction is pending.
{% from 'redpanda/map.jinja' import rp with context %}
{% set initialized = salt['redpanda.initialized']() and not salt['redpanda.bootstrap_needed']() %}
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
