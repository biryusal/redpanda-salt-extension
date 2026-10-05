{% from 'redpanda/map.jinja' import rp with context %}
{% if rp.get('create_pkg_mgr_proxy') and rp.get('https_proxy') %}
{% if grains.os_family == 'Debian' %}
redpanda-apt-proxy:
  file.managed:
    - name: /etc/apt/apt.conf.d/80-redpanda-proxy
    - contents: {{ ('Acquire::http::Proxy ' + (rp.https_proxy | tojson) + ';\nAcquire::https::Proxy ' + (rp.https_proxy | tojson) + ';') | tojson }}
    - mode: '0600'
    - show_changes: false
    - require:
      - module: redpanda-rollout-guard
    - require_in:
      - pkg: redpanda-dependencies
{% else %}
redpanda-dnf-proxy:
  ini.options_present:
    - name: /etc/dnf/dnf.conf
    - sections:
        main:
          proxy: {{ rp.https_proxy | tojson }}
    - require:
      - module: redpanda-rollout-guard
    - require_in:
      - pkg: redpanda-dependencies
{% endif %}
{% endif %}
