{% from 'redpanda/map.jinja' import rp with context %}
{% set devices = rp.storage.devices %}
{% if devices %}
{% set device = devices[0] if devices | length == 1 else '/dev/md0' %}
redpanda-storage-devices:
  module.run:
    - name: redpanda.validate_storage_devices
    - require:
      - pkg: redpanda-dependencies
    - failhard: true
{% if devices | length > 1 %}
redpanda-raid:
  raid.present:
    - name: /dev/md0
    - level: 0
    - devices: {{ devices | tojson }}
    - require:
      - pkg: redpanda-dependencies
      - module: redpanda-storage-devices
{% endif %}
{% if rp.storage.format %}
redpanda-xfs:
  cmd.run:
    - name: {{ ('mkfs.xfs -m rmapbt=0,uuid=' + rp.storage.uuid + ' ' + device) | tojson }}
    - unless: {{ ('blkid ' + device) | tojson }}
    - python_shell: false
    - require:
      - pkg: redpanda-dependencies
      - module: redpanda-storage-devices
{% if devices | length > 1 %}
      - raid: redpanda-raid
{% endif %}
{% endif %}
redpanda-storage-uuid:
  module.run:
    - name: redpanda.validate_storage
    - require:
      - pkg: redpanda-dependencies
      - module: redpanda-storage-devices
{% if rp.storage.format %}
      - cmd: redpanda-xfs
{% endif %}
{% if devices | length > 1 %}
      - raid: redpanda-raid
{% endif %}
redpanda-data-mount:
  mount.mounted:
    - name: {{ rp.storage.get('mountpoint', '/mnt/vectorized') | tojson }}
    - device: {{ ('UUID=' + rp.storage.uuid) | tojson }}
    - fstype: xfs
    - opts: {{ (['defaults', 'x-systemd.device-timeout=' + rp.storage.get('device_timeout', '15s')] + (['nofail'] if rp.storage.get('ephemeral', false) else [])) | tojson }}
    - dump: {{ 1 if rp.storage.get('ephemeral', false) else 0 }}
    - pass_num: {{ 2 if rp.storage.get('ephemeral', false) else 0 }}
    - mkmnt: true
    - persist: true
    - require:
      - module: redpanda-storage-uuid
      - pkg: redpanda-dependencies
      - module: redpanda-storage-devices
{% if rp.storage.format %}
      - cmd: redpanda-xfs
{% endif %}
{% if devices | length > 1 %}
      - raid: redpanda-raid
{% endif %}
{% endif %}
redpanda-data-directory:
  file.directory:
    - name: {{ rp.data_directory | tojson }}
    - user: {{ rp.user | tojson }}
    - group: {{ rp.group | tojson }}
    - mode: '0750'
    - makedirs: true
    - require:
      - user: redpanda-user
{% if devices %}
      - mount: redpanda-data-mount
{% endif %}
