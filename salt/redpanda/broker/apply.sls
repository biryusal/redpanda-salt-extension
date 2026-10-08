# Broker update: prepare → drain → packages → configuration → service → complete.
# Cross-file require edges enforce this order; include order alone does not.
include:
  - redpanda.broker.prepare
  - redpanda.broker.drain
  - redpanda.broker.packages
  - redpanda.broker.configuration
  - redpanda.broker.service
  - redpanda.broker.complete
