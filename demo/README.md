# glide-mq Demo Application

Comprehensive demonstration of all glide-mq features simulating a complete e-commerce platform.

## Features Demonstrated

### Core Queue Operations
- Job processing with progress tracking
- Bulk operations with Batch API
- Priority queues with FIFO ordering
- Delayed/scheduled jobs
- Job retries with exponential backoff
- Dead letter queue handling

### Advanced Features
- Deduplication (simple, throttle, debounce)
- Rate limiting (sliding window)
- Global concurrency limits
- Job retention policies
- Transparent gzip compression
- Timeout handling

### Workflows
- FlowProducer for parent-child job trees
- Chain pattern for sequential pipelines
- Group pattern for parallel execution
- Complex nested workflows

### Observability
- Real-time event streaming
- Progress tracking
- Metrics and job counts
- Job logs and state tracking
- Dashboard API integration

## Setup

1. Install dependencies:
```bash
cd demo
npm install
```

2. Ensure Valkey/Redis is running:
```bash
# Single node on port 6379
valkey-server

# Or for cluster mode (optional)
# Ports 7000-7005
```

## Running the Demo

### Option 1: Full Demo with All Features
```bash
npm start
```

This launches:
- 10 different queue types (orders, payments, inventory, etc.)
- 10 specialized workers with different configurations
- 12 demo scenarios showcasing various features
- Real-time metrics display every 5 seconds

### Option 2: Dashboard Server
```bash
npm run dashboard
```

Opens dashboard API server on http://localhost:3000

Features:
- REST API for queue management
- Server-Sent Events for real-time updates
- Queue pause/resume controls
- Job retry/remove operations
- Metrics and monitoring

## Demo Scenarios

1. **Simple Order Processing** - Basic job with progress tracking
2. **Bulk Inventory Update** - 20 jobs added via addBulk
3. **Priority Tasks** - Jobs with different priority levels
4. **Scheduled Notifications** - Delayed job execution
5. **Payment with Retries** - Automatic retry on failure
6. **Deduplicated Analytics** - Prevents duplicate events
7. **E-commerce Workflow** - Complex parent-child job tree
8. **Sequential Pipeline** - Chain pattern for data processing
9. **Parallel Broadcast** - Group pattern for notifications
10. **Large Report Generation** - Compression for big payloads
11. **Rate-Limited Recommendations** - Throttled processing
12. **Timeout Handling** - Job timeout demonstration

## Queue Types

- **orders** - Order processing with progress tracking
- **payments** - Payment processing with retry logic
- **inventory** - Rate-limited inventory updates
- **shipping** - Shipping label generation
- **notifications** - Multi-channel notifications (email/SMS/push)
- **analytics** - Event aggregation and metrics
- **recommendations** - ML recommendation generation
- **reports** - Long-running report generation
- **priority-tasks** - Priority-based task execution
- **dead-letter** - Failed job investigation

## API Endpoints (Dashboard Server)

- `GET /api/queues` - List all queues with counts
- `GET /api/queues/:name` - Queue details and jobs
- `GET /api/queues/:name/jobs/:id` - Specific job details
- `POST /api/queues/:name/jobs` - Add new job
- `POST /api/queues/:name/pause` - Pause queue
- `POST /api/queues/:name/resume` - Resume queue
- `POST /api/queues/:name/jobs/:id/retry` - Retry failed job
- `DELETE /api/queues/:name/jobs/:id` - Remove job
- `POST /api/queues/:name/drain` - Drain queue
- `POST /api/queues/:name/obliterate` - Obliterate queue
- `GET /api/events` - SSE stream for real-time updates

## Monitoring

The demo displays real-time metrics including:
- Queue counts (waiting, active, delayed, completed, failed)
- Job progress updates
- Success/failure events
- Stalled job detection
- Worker status

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Producer  │────▶│    Queue    │────▶│   Worker    │
│  (Demo App) │     │   (Valkey)  │     │ (Processor) │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  Dashboard  │
                    │   (API/UI)  │
                    └─────────────┘
```

## Customization

Edit `index.ts` to:
- Add new queue types
- Modify worker configurations
- Create custom scenarios
- Adjust timing and concurrency
- Change retry strategies

## Troubleshooting

- Ensure Valkey/Redis is running on localhost:6379
- Check Node.js version is 20+
- Verify all dependencies installed
- Monitor console for error messages
- Use dashboard API to inspect job details

## Performance Tips

- Increase worker concurrency for higher throughput
- Use bulk operations for batch processing
- Enable compression for large payloads
- Configure appropriate retention policies
- Use priority queues for critical tasks