# Handover

## Current State (2026-02-15)

### Published packages
- **glide-mq@0.1.0** on npm
- **@glidemq/speedkey@0.2.0** on npm (7 platform binaries including Windows)

### Repos
- github.com/avifenesh/glide-mq (queue library)
- github.com/avifenesh/speedkey (native Valkey client)
- github.com/avifenesh/glide-mq-demo (e-commerce demo app)

### Performance
- c=1: 4,376 jobs/s (2.1x BullMQ)
- c=10: 20,979 jobs/s (10x BullMQ)
- c=50: 44,643 jobs/s

### CI
- glide-mq: typecheck + unit tests + integration tests (all green)
- speedkey: 6-target CD with platform package publishing

### Testing
- 750+ test executions (standalone + cluster parameterized)
- 44 bulletproof tests (known bugs from BullMQ, Celery, Sidekiq, Bee-Queue)
- 20 fuzz/chaos tests
- 4-pass code review (73 findings addressed)
- Demo app validates full pipeline end-to-end

### NPM token
- Needs rotation (was shared in conversation)
