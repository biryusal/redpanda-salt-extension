{% if salt['redpanda.bootstrap_needed']() %}
include:
  - redpanda.redpanda_broker.apply
{% else %}
redpanda-existing-broker:
  test.nop:
    - name: Existing broker is deferred to the serial apply phase
{% endif %}
