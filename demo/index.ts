#!/usr/bin/env node
import { Queue, Worker, FlowProducer, QueueEvents, chain, group } from 'glide-mq';
import type { Job } from 'glide-mq';
import chalk from 'chalk';
import ora from 'ora';
import Table from 'cli-table3';
import { setTimeout } from 'timers/promises';

// Connection config
const connection = {
  addresses: [{ host: 'localhost', port: 6379 }],
  clusterMode: false,
};

// Color-coded console logging
const log = {
  info: (msg: string) => console.log(chalk.cyan('[INFO]'), msg),
  success: (msg: string) => console.log(chalk.green('[SUCCESS]'), msg),
  error: (msg: string) => console.log(chalk.red('[ERROR]'), msg),
  warn: (msg: string) => console.log(chalk.yellow('[WARN]'), msg),
  job: (msg: string) => console.log(chalk.magenta('[JOB]'), msg),
  flow: (msg: string) => console.log(chalk.blue('[FLOW]'), msg),
};

// Queue definitions with different features
const queues = {
  orders: new Queue('orders', { connection, compression: 'gzip' }),
  payments: new Queue('payments', { connection }),
  inventory: new Queue('inventory', { connection }),
  shipping: new Queue('shipping', { connection }),
  notifications: new Queue('notifications', { connection }),
  analytics: new Queue('analytics', { connection }),
  recommendations: new Queue('recommendations', { connection }),
  reports: new Queue('reports', { connection }),
  deadLetter: new Queue('dead-letter', { connection }),
  priority: new Queue('priority-tasks', { connection }),
};

// Flow producer for complex workflows
const flowProducer = new FlowProducer({ connection });

// Event listeners for all queues
const setupQueueEvents = () => {
  Object.entries(queues).forEach(([name, queue]) => {
    const events = new QueueEvents(queue.name, { connection });

    events.on('added', ({ jobId }) => {
      log.job(`${name}: Job ${jobId} added`);
    });

    events.on('completed', ({ jobId, returnvalue }) => {
      log.success(`${name}: Job ${jobId} completed with result: ${JSON.stringify(returnvalue)}`);
    });

    events.on('failed', ({ jobId, failedReason }) => {
      log.error(`${name}: Job ${jobId} failed - ${failedReason}`);
    });

    events.on('progress', ({ jobId, data }) => {
      log.info(`${name}: Job ${jobId} progress: ${data}%`);
    });

    events.on('stalled', ({ jobId }) => {
      log.warn(`${name}: Job ${jobId} stalled`);
    });
  });
};

// Worker implementations
const setupWorkers = () => {
  // Order processing worker with progress tracking
  const orderWorker = new Worker(
    'orders',
    async (job: Job) => {
      await job.log(`Processing order ${job.data.orderId}`);
      await job.updateProgress(25);

      // Simulate order validation
      await setTimeout(500);
      await job.updateProgress(50);

      // Simulate inventory check
      await setTimeout(500);
      await job.updateProgress(75);

      // Complete order
      await setTimeout(500);
      await job.updateProgress(100);

      return {
        orderId: job.data.orderId,
        status: 'processed',
        items: job.data.items,
        total: job.data.total,
      };
    },
    {
      connection,
      concurrency: 5,
      stalledInterval: 30000,
      deadLetterQueue: { name: 'dead-letter' },
    },
  );

  // Payment processor with retries
  const paymentWorker = new Worker(
    'payments',
    async (job: Job) => {
      await job.log(`Processing payment for order ${job.data.orderId}`);

      // Simulate payment gateway call
      await setTimeout(1000);

      // Random failure for retry demonstration
      if (Math.random() < 0.2 && job.attemptsMade < 2) {
        throw new Error('Payment gateway timeout');
      }

      return {
        orderId: job.data.orderId,
        transactionId: `TXN-${Date.now()}`,
        amount: job.data.amount,
        status: 'completed',
      };
    },
    {
      connection,
      concurrency: 3,
      backoffStrategies: {
        exponential: (attemptsMade) => 2 ** attemptsMade * 1000,
      },
    },
  );

  // Inventory worker with rate limiting
  const inventoryWorker = new Worker(
    'inventory',
    async (job: Job) => {
      await job.log(`Updating inventory for SKUs: ${job.data.skus.join(', ')}`);
      await setTimeout(300);

      return {
        updated: job.data.skus.length,
        timestamp: new Date().toISOString(),
      };
    },
    {
      connection,
      concurrency: 10,
      limiter: { max: 50, duration: 60000 }, // 50 jobs per minute
    },
  );

  // Shipping worker
  const shippingWorker = new Worker(
    'shipping',
    async (job: Job) => {
      await job.log(`Creating shipping label for order ${job.data.orderId}`);
      await setTimeout(800);

      return {
        orderId: job.data.orderId,
        trackingNumber: `TRACK-${Date.now()}`,
        carrier: ['FedEx', 'UPS', 'USPS'][Math.floor(Math.random() * 3)],
        estimatedDelivery: new Date(Date.now() + 3 * 24 * 60 * 60 * 1000).toISOString(),
      };
    },
    {
      connection,
      concurrency: 5,
    },
  );

  // Notification worker with multiple channels
  const notificationWorker = new Worker(
    'notifications',
    async (job: Job) => {
      const { type, recipient, message } = job.data;
      await job.log(`Sending ${type} notification to ${recipient}`);

      // Simulate different notification channels
      const delay = type === 'email' ? 500 : type === 'sms' ? 300 : 100;
      await setTimeout(delay);

      return {
        sent: true,
        channel: type,
        recipient,
        timestamp: new Date().toISOString(),
      };
    },
    {
      connection,
      concurrency: 20,
    },
  );

  // Analytics worker for aggregation
  const analyticsWorker = new Worker(
    'analytics',
    async (job: Job) => {
      await job.log(`Processing analytics event: ${job.data.event}`);
      await setTimeout(200);

      return {
        event: job.data.event,
        processed: true,
        metrics: {
          views: Math.floor(Math.random() * 1000),
          clicks: Math.floor(Math.random() * 100),
          conversions: Math.floor(Math.random() * 10),
        },
      };
    },
    {
      connection,
      concurrency: 15,
    },
  );

  // Recommendation engine worker
  const recommendationWorker = new Worker(
    'recommendations',
    async (job: Job) => {
      await job.log(`Generating recommendations for user ${job.data.userId}`);
      await setTimeout(1500);

      return {
        userId: job.data.userId,
        recommendations: [
          { productId: 'PROD-001', score: 0.95 },
          { productId: 'PROD-002', score: 0.87 },
          { productId: 'PROD-003', score: 0.82 },
        ],
      };
    },
    {
      connection,
      concurrency: 2,
    },
  );

  // Report generator with long-running tasks
  const reportWorker = new Worker(
    'reports',
    async (job: Job) => {
      await job.log(`Generating ${job.data.type} report`);

      // Simulate long-running report generation
      for (let i = 0; i <= 100; i += 10) {
        await setTimeout(500);
        await job.updateProgress(i);
      }

      return {
        reportId: `REPORT-${Date.now()}`,
        type: job.data.type,
        url: `https://reports.example.com/${Date.now()}.pdf`,
        pages: Math.floor(Math.random() * 50) + 10,
      };
    },
    {
      connection,
      concurrency: 1,
    },
  );

  // Priority task worker
  const priorityWorker = new Worker(
    'priority-tasks',
    async (job: Job) => {
      await job.log(`Processing priority task: ${job.data.task}`);
      await setTimeout(100);

      return {
        task: job.data.task,
        priority: job.opts.priority,
        completed: new Date().toISOString(),
      };
    },
    {
      connection,
      concurrency: 10,
    },
  );

  // Dead letter queue worker for investigation
  const deadLetterWorker = new Worker(
    'dead-letter',
    async (job: Job) => {
      await job.log(`Investigating failed job from ${job.data.originalQueue}`);
      // Here you would typically log to external system or alert
      return { investigated: true };
    },
    {
      connection,
      concurrency: 1,
    },
  );

  return {
    orderWorker,
    paymentWorker,
    inventoryWorker,
    shippingWorker,
    notificationWorker,
    analyticsWorker,
    recommendationWorker,
    reportWorker,
    priorityWorker,
    deadLetterWorker,
  };
};

// Demo scenarios
async function runDemoScenarios() {
  const spinner = ora('Starting demo scenarios...').start();

  // Scenario 1: Simple job with progress
  spinner.text = 'Scenario 1: Simple order processing';
  await queues.orders.add('process-order', {
    orderId: 'ORD-001',
    items: ['SKU-123', 'SKU-456'],
    total: 299.99,
  });
  await setTimeout(2000);

  // Scenario 2: Bulk operations
  spinner.text = 'Scenario 2: Bulk inventory update';
  const bulkJobs = Array.from({ length: 20 }, (_, i) => ({
    name: 'update-inventory',
    data: { skus: [`SKU-${i}00`, `SKU-${i}01`, `SKU-${i}02`] },
  }));
  await queues.inventory.addBulk(bulkJobs);
  await setTimeout(1000);

  // Scenario 3: Priority jobs
  spinner.text = 'Scenario 3: Priority tasks';
  await queues.priority.add('urgent-task', { task: 'Critical system update' }, { priority: 1 });
  await queues.priority.add('normal-task', { task: 'Regular maintenance' }, { priority: 5 });
  await queues.priority.add('low-task', { task: 'Cleanup operation' }, { priority: 10 });
  await setTimeout(1000);

  // Scenario 4: Delayed jobs
  spinner.text = 'Scenario 4: Scheduled notifications';
  await queues.notifications.add(
    'reminder',
    { type: 'email', recipient: 'user@example.com', message: 'Order shipped!' },
    { delay: 5000 },
  );
  await setTimeout(1000);

  // Scenario 5: Job with retries
  spinner.text = 'Scenario 5: Payment processing with retries';
  await queues.payments.add(
    'process-payment',
    { orderId: 'ORD-002', amount: 199.99 },
    { attempts: 3, backoff: { type: 'exponential', delay: 1000 } },
  );
  await setTimeout(2000);

  // Scenario 6: Deduplication
  spinner.text = 'Scenario 6: Deduplicated analytics events';
  for (let i = 0; i < 5; i++) {
    await queues.analytics.add(
      'track-event',
      { event: 'page_view', userId: 'USER-123', page: '/home' },
      { deduplication: { id: 'pageview-home-123', mode: 'simple' } },
    );
  }
  await setTimeout(1000);

  // Scenario 7: Complex workflow with FlowProducer
  spinner.text = 'Scenario 7: E-commerce order workflow';
  await flowProducer.add({
    name: 'complete-order',
    queueName: 'orders',
    data: { orderId: 'ORD-003', items: ['PROD-A', 'PROD-B'], total: 499.99 },
    children: [
      {
        name: 'process-payment',
        queueName: 'payments',
        data: { orderId: 'ORD-003', amount: 499.99 },
        children: [
          {
            name: 'update-inventory',
            queueName: 'inventory',
            data: { skus: ['PROD-A', 'PROD-B'] },
          },
          {
            name: 'create-shipping',
            queueName: 'shipping',
            data: { orderId: 'ORD-003', address: '123 Main St' },
          },
        ],
      },
      {
        name: 'send-confirmation',
        queueName: 'notifications',
        data: { type: 'email', recipient: 'customer@example.com', message: 'Order confirmed!' },
      },
    ],
  });
  await setTimeout(3000);

  // Scenario 8: Chain pattern - Sequential pipeline
  spinner.text = 'Scenario 8: Sequential data pipeline';
  await chain(
    'analytics',
    [
      { name: 'collect-data', data: { source: 'api', endpoint: '/metrics' } },
      { name: 'transform-data', data: { format: 'json' } },
      { name: 'aggregate-data', data: { window: '1h' } },
      { name: 'store-data', data: { destination: 'warehouse' } },
    ],
    connection,
  );
  await setTimeout(2000);

  // Scenario 9: Group pattern - Parallel execution
  spinner.text = 'Scenario 9: Parallel notification broadcast';
  await group(
    'notifications',
    [
      {
        name: 'send-email',
        data: { type: 'email', recipient: 'user1@example.com', message: 'New feature!' },
      },
      {
        name: 'send-sms',
        data: { type: 'sms', recipient: '+1234567890', message: 'New feature!' },
      },
      {
        name: 'send-push',
        data: { type: 'push', recipient: 'device-token-123', message: 'New feature!' },
      },
    ],
    connection,
  );
  await setTimeout(2000);

  // Scenario 10: Large payload with compression
  spinner.text = 'Scenario 10: Large report generation with compression';
  const largeData = {
    reportType: 'annual',
    data: Array.from({ length: 1000 }, (_, i) => ({
      id: i,
      timestamp: new Date().toISOString(),
      metrics: {
        revenue: Math.random() * 10000,
        orders: Math.floor(Math.random() * 100),
        customers: Math.floor(Math.random() * 50),
      },
    })),
  };
  await queues.reports.add('generate-annual-report', largeData);
  await setTimeout(3000);

  // Scenario 11: Rate-limited batch processing
  spinner.text = 'Scenario 11: Rate-limited recommendation generation';
  const userIds = Array.from({ length: 10 }, (_, i) => `USER-${i}`);
  for (const userId of userIds) {
    await queues.recommendations.add('generate-recommendations', { userId });
  }
  await setTimeout(5000);

  // Scenario 12: Job with timeout demonstration
  spinner.text = 'Scenario 12: Job timeout handling';
  await queues.reports.add(
    'slow-report',
    { type: 'detailed-analysis' },
    { timeout: 3000 }, // Will timeout if takes > 3s
  );
  await setTimeout(2000);

  spinner.succeed('All demo scenarios launched!');
}

// Display metrics
async function displayMetrics() {
  console.log('\n' + chalk.bold.cyan('Queue Metrics:'));

  const table = new Table({
    head: ['Queue', 'Waiting', 'Active', 'Delayed', 'Completed', 'Failed'],
    colWidths: [20, 10, 10, 10, 12, 10],
    style: { head: ['cyan'] },
  });

  for (const [name, queue] of Object.entries(queues)) {
    const counts = await queue.getJobCounts();
    table.push([
      name,
      counts.waiting || 0,
      counts.active || 0,
      counts.delayed || 0,
      counts.completed || 0,
      counts.failed || 0,
    ]);
  }

  console.log(table.toString());
}

// Main execution
async function main() {
  console.log(chalk.bold.green('\n[OK] Starting glide-mq Comprehensive Demo\n'));
  console.log(chalk.gray('This demo showcases all major features of glide-mq:\n'));
  console.log(chalk.gray('- Job processing with progress tracking'));
  console.log(chalk.gray('- Bulk operations and compression'));
  console.log(chalk.gray('- Priority queues and delayed jobs'));
  console.log(chalk.gray('- Retries with backoff strategies'));
  console.log(chalk.gray('- Deduplication and rate limiting'));
  console.log(chalk.gray('- Complex workflows and patterns'));
  console.log(chalk.gray('- Dead letter queue handling'));
  console.log(chalk.gray('- And much more...\n'));

  // Setup
  setupQueueEvents();
  const workers = setupWorkers();

  // Run demo scenarios
  await runDemoScenarios();

  // Display metrics periodically
  const metricsInterval = setInterval(displayMetrics, 5000);
  await displayMetrics();

  // Keep running
  console.log(chalk.green('\n[OK] Demo is running. Press Ctrl+C to stop.\n'));

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log(chalk.yellow('\n[WARN] Shutting down gracefully...'));
    clearInterval(metricsInterval);

    // Close all workers
    await Promise.all(Object.values(workers).map((w) => w.close()));

    // Close all queues
    await Promise.all(Object.values(queues).map((q) => q.close()));

    console.log(chalk.green('[OK] Demo stopped.'));
    process.exit(0);
  });
}

// Error handling
process.on('unhandledRejection', (err) => {
  log.error(`Unhandled rejection: ${err}`);
  console.error(err);
});

// Start the demo
main().catch(console.error);
