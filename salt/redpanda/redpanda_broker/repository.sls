{% from 'redpanda/map.jinja' import rp with context %}
{% if not rp.airgap %}
{% if rp.development_build %}
redpanda-repository:
  pkgrepo.managed:
    - name: {{ rp.nightly_repository | tojson }}
{% if grains.os_family == 'Debian' %}
    - file: /etc/apt/sources.list.d/redpanda-nightly.list
    - aptkey: false
    - key_url: {{ rp.nightly_key_url | tojson }}
{% else %}
    - baseurl: {{ rp.nightly_baseurl | tojson }}
    - gpgkey: {{ rp.nightly_key_url | tojson }}
    - gpgcheck: 1
{% endif %}
    - require:
      - pkg: redpanda-dependencies
{% elif grains.os_family == 'Debian' %}
redpanda-repository:
  pkgrepo.managed:
    - name: {{ ('deb [signed-by=/usr/share/keyrings/redpanda-redpanda-archive-keyring.gpg] ' + rp.base_url + '/apt redpanda-' + ('unstable-' if rp.unstable else '') + 'apt main') | tojson }}
    - file: /etc/apt/sources.list.d/redpanda.list
    - key_url: {{ (rp.base_url + '/redpanda-deb-signing-public.gpg') | tojson }}
    - aptkey: false
    - refresh: true
    - require:
      - pkg: redpanda-dependencies
{% else %}
{% set channel = 'redpanda-unstable-yum' if rp.unstable else 'redpanda-yum' %}
redpanda-repository:
  pkgrepo.managed:
    - name: redpanda
    - humanname: Redpanda
    - baseurl: {{ (rp.base_url + '/yum/' + channel) | tojson }}
    - gpgkey: {{ [rp.base_url + '/redpanda-rpm-signing-public.key', rp.base_url + '/yum/' + channel + '/repodata/repomd.xml.key'] | tojson }}
    - gpgcheck: 1
    - repo_gpgcheck: {{ 0 if salt['file.file_exists']('/usr/bin/dnf5') else 1 }}
    - sslverify: 1
    - require:
      - pkg: redpanda-dependencies
{% endif %}
{% endif %}
