{% if salt['redpanda.recovery_needed']() %}
include:
  - redpanda.redpanda_broker.apply
{% else %}
redpanda-no-pending-recovery:
  test.nop:
    - name: No interrupted update on this broker
{% endif %}
