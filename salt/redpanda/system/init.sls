# Prepare the host before broker packages and configuration are applied.
include:
  - redpanda.system.validate
  - redpanda.system.packages
  - redpanda.system.user
  - redpanda.system.directories
  - redpanda.system.storage
  - redpanda.system.proxy
  - redpanda.system.sysctl
