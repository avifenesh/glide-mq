#!/usr/bin/env node
import express from 'express';
import { Queue, Worker, QueueEvents } from 'glide-mq';
import chalk from 'chalk';

const app = express();
const PORT = 3000;

// Connection config
const connection = {
  addresses: [{ host: 'localhost', port: 6379 }],
  clusterMode: false,
};

// Queue registry for dashboard
const queueRegistry = [
  'orders',
  'payments',
  'inventory',
  'shipping',
  'notifications',
  'analytics',
  'recommendations',
  'reports',
  'dead-letter',
  'priority-tasks',
];

// Create queue instances for dashboard
const queues = queueRegistry.reduce(
  (acc, name) => {
    acc[name] = new Queue(name, { connection });
    return acc;
  },
  {} as Record<string, Queue>,
);

// Middleware for CORS (if dashboard is on different port)
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  next();
});

app.use(express.json());

// Dashboard API endpoints
app.get('/api/queues', async (req, res) => {
  const queueData = await Promise.all(
    Object.entries(queues).map(async ([name, queue]) => {
      const counts = await queue.getJobCounts();
      const metrics = await queue.getMetrics('completed', 'failed');

      return {
        name,
        counts,
        metrics: {
          completed: metrics.completed || { count: 0, prevCount: 0 },
          failed: metrics.failed || { count: 0, prevCount: 0 },
        },
        isPaused: await queue.isPaused(),
      };
    }),
  );

  res.json(queueData);
});

// Get specific queue details
app.get('/api/queues/:name', async (req, res) => {
  const { name } = req.params;
  const queue = queues[name];

  if (!queue) {
    return res.status(404).json({ error: 'Queue not found' });
  }

  const counts = await queue.getJobCounts();
  const jobs = await queue.getJobs(['waiting', 'active', 'completed', 'failed', 'delayed'], 0, 20);

  res.json({
    name,
    counts,
    jobs: jobs.map((job) => ({
      id: job.id,
      name: job.name,
      data: job.data,
      opts: job.opts,
      progress: job.progress,
      attemptsMade: job.attemptsMade,
      failedReason: job.failedReason,
      finishedOn: job.finishedOn,
      processedOn: job.processedOn,
      timestamp: job.timestamp,
    })),
  });
});

// Get specific job
app.get('/api/queues/:queueName/jobs/:jobId', async (req, res) => {
  const { queueName, jobId } = req.params;
  const queue = queues[queueName];

  if (!queue) {
    return res.status(404).json({ error: 'Queue not found' });
  }

  const job = await queue.getJob(jobId);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  const state = await job.getState();
  const logs = await job.getLogs();

  res.json({
    id: job.id,
    name: job.name,
    data: job.data,
    opts: job.opts,
    progress: job.progress,
    attemptsMade: job.attemptsMade,
    failedReason: job.failedReason,
    finishedOn: job.finishedOn,
    processedOn: job.processedOn,
    timestamp: job.timestamp,
    state,
    logs: logs.logs,
    returnvalue: job.returnvalue,
  });
});

// Pause queue
app.post('/api/queues/:name/pause', async (req, res) => {
  const { name } = req.params;
  const queue = queues[name];

  if (!queue) {
    return res.status(404).json({ error: 'Queue not found' });
  }

  await queue.pause();
  res.json({ success: true, message: `Queue ${name} paused` });
});

// Resume queue
app.post('/api/queues/:name/resume', async (req, res) => {
  const { name } = req.params;
  const queue = queues[name];

  if (!queue) {
    return res.status(404).json({ error: 'Queue not found' });
  }

  await queue.resume();
  res.json({ success: true, message: `Queue ${name} resumed` });
});

// Add job to queue
app.post('/api/queues/:name/jobs', async (req, res) => {
  const { name } = req.params;
  const { jobName, data, opts } = req.body;
  const queue = queues[name];

  if (!queue) {
    return res.status(404).json({ error: 'Queue not found' });
  }

  const job = await queue.add(jobName || 'manual-job', data || {}, opts || {});
  res.json({ success: true, jobId: job.id });
});

// Retry failed job
app.post('/api/queues/:queueName/jobs/:jobId/retry', async (req, res) => {
  const { queueName, jobId } = req.params;
  const queue = queues[queueName];

  if (!queue) {
    return res.status(404).json({ error: 'Queue not found' });
  }

  const job = await queue.getJob(jobId);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  await job.retry();
  res.json({ success: true, message: `Job ${jobId} retried` });
});

// Remove job
app.delete('/api/queues/:queueName/jobs/:jobId', async (req, res) => {
  const { queueName, jobId } = req.params;
  const queue = queues[queueName];

  if (!queue) {
    return res.status(404).json({ error: 'Queue not found' });
  }

  const job = await queue.getJob(jobId);
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }

  await job.remove();
  res.json({ success: true, message: `Job ${jobId} removed` });
});

// Drain queue
app.post('/api/queues/:name/drain', async (req, res) => {
  const { name } = req.params;
  const queue = queues[name];

  if (!queue) {
    return res.status(404).json({ error: 'Queue not found' });
  }

  await queue.drain();
  res.json({ success: true, message: `Queue ${name} drained` });
});

// Obliterate queue
app.post('/api/queues/:name/obliterate', async (req, res) => {
  const { name } = req.params;
  const queue = queues[name];

  if (!queue) {
    return res.status(404).json({ error: 'Queue not found' });
  }

  await queue.obliterate({ force: true });
  res.json({ success: true, message: `Queue ${name} obliterated` });
});

// SSE endpoint for real-time updates
app.get('/api/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    Connection: 'keep-alive',
  });

  // Setup event listeners for all queues
  const eventSources = queueRegistry.map((name) => {
    const events = new QueueEvents(name, { connection });

    events.on('added', ({ jobId }) => {
      res.write(`data: ${JSON.stringify({ queue: name, event: 'added', jobId })}\n\n`);
    });

    events.on('completed', ({ jobId, returnvalue }) => {
      res.write(
        `data: ${JSON.stringify({ queue: name, event: 'completed', jobId, returnvalue })}\n\n`,
      );
    });

    events.on('failed', ({ jobId, failedReason }) => {
      res.write(
        `data: ${JSON.stringify({ queue: name, event: 'failed', jobId, failedReason })}\n\n`,
      );
    });

    events.on('progress', ({ jobId, data }) => {
      res.write(
        `data: ${JSON.stringify({ queue: name, event: 'progress', jobId, progress: data })}\n\n`,
      );
    });

    events.on('stalled', ({ jobId }) => {
      res.write(`data: ${JSON.stringify({ queue: name, event: 'stalled', jobId })}\n\n`);
    });

    return events;
  });

  // Heartbeat
  const heartbeat = setInterval(() => {
    res.write(`data: ${JSON.stringify({ event: 'heartbeat', timestamp: Date.now() })}\n\n`);
  }, 30000);

  // Cleanup on close
  req.on('close', () => {
    clearInterval(heartbeat);
    eventSources.forEach((events) => events.close());
  });
});

// Serve a basic HTML dashboard (placeholder until @glidemq/dashboard is available)
app.get('/', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
  <title>glide-mq Dashboard</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .container {
      background: white;
      border-radius: 20px;
      box-shadow: 0 20px 60px rgba(0,0,0,0.3);
      padding: 40px;
      max-width: 800px;
      width: 90%;
    }
    h1 {
      color: #333;
      margin-bottom: 10px;
      font-size: 2.5em;
    }
    .subtitle {
      color: #666;
      margin-bottom: 30px;
      font-size: 1.1em;
    }
    .status {
      background: #f0f4f8;
      border-radius: 10px;
      padding: 20px;
      margin-bottom: 20px;
    }
    .status-item {
      display: flex;
      justify-content: space-between;
      margin: 10px 0;
      padding: 10px;
      background: white;
      border-radius: 5px;
    }
    .label {
      font-weight: 600;
      color: #555;
    }
    .value {
      color: #667eea;
      font-weight: bold;
    }
    .instructions {
      background: #fef3c7;
      border: 2px solid #f59e0b;
      border-radius: 10px;
      padding: 20px;
      margin-top: 30px;
    }
    .instructions h3 {
      color: #92400e;
      margin-bottom: 10px;
    }
    .instructions p {
      color: #78350f;
      line-height: 1.6;
    }
    .instructions code {
      background: #fed7aa;
      padding: 2px 6px;
      border-radius: 3px;
      font-family: 'Courier New', monospace;
    }
    .endpoint-list {
      margin-top: 20px;
    }
    .endpoint {
      background: white;
      padding: 8px 12px;
      margin: 5px 0;
      border-radius: 5px;
      font-family: monospace;
      color: #059669;
    }
  </style>
</head>
<body>
  <div class="container">
    <h1>glide-mq Dashboard</h1>
    <p class="subtitle">High-performance message queue monitoring</p>

    <div class="status">
      <h2>API Status</h2>
      <div class="status-item">
        <span class="label">Server</span>
        <span class="value">Running on port ${PORT}</span>
      </div>
      <div class="status-item">
        <span class="label">Valkey Connection</span>
        <span class="value">Connected</span>
      </div>
      <div class="status-item">
        <span class="label">Queues Registered</span>
        <span class="value">${queueRegistry.length}</span>
      </div>
    </div>

    <div class="instructions">
      <h3>Dashboard Setup</h3>
      <p>
        This server provides REST API endpoints for the glide-mq dashboard.
        Once <code>@glidemq/dashboard</code> is available, install it with:
      </p>
      <p style="margin: 15px 0;">
        <code>npm install @glidemq/dashboard</code>
      </p>
      <p>
        Then the dashboard UI will be available at this URL.
        For now, you can use the API endpoints directly:
      </p>

      <div class="endpoint-list">
        <div class="endpoint">GET /api/queues</div>
        <div class="endpoint">GET /api/queues/:name</div>
        <div class="endpoint">GET /api/queues/:name/jobs/:id</div>
        <div class="endpoint">POST /api/queues/:name/jobs</div>
        <div class="endpoint">POST /api/queues/:name/pause</div>
        <div class="endpoint">POST /api/queues/:name/resume</div>
        <div class="endpoint">GET /api/events (SSE)</div>
      </div>
    </div>
  </div>

  <script>
    // Auto-refresh queue counts
    async function updateStatus() {
      try {
        const response = await fetch('/api/queues');
        const queues = await response.json();
        console.log('Queue status:', queues);
      } catch (err) {
        console.error('Failed to fetch queue status:', err);
      }
    }

    // Connect to SSE for real-time updates
    const eventSource = new EventSource('/api/events');
    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      console.log('Real-time event:', data);
    };

    // Update every 5 seconds
    setInterval(updateStatus, 5000);
    updateStatus();
  </script>
</body>
</html>
  `);
});

// Start server
app.listen(PORT, () => {
  console.log(chalk.green(`[OK] Dashboard server running at http://localhost:${PORT}`));
  console.log(chalk.cyan('[INFO] API endpoints available at http://localhost:' + PORT + '/api'));
  console.log(chalk.yellow('[WARN] Full dashboard UI pending @glidemq/dashboard package'));
  console.log(chalk.gray('\nAvailable endpoints:'));
  console.log(chalk.gray('  GET  /api/queues              - List all queues'));
  console.log(chalk.gray('  GET  /api/queues/:name        - Queue details'));
  console.log(chalk.gray('  GET  /api/queues/:n/jobs/:id  - Job details'));
  console.log(chalk.gray('  POST /api/queues/:name/jobs   - Add job'));
  console.log(chalk.gray('  POST /api/queues/:name/pause  - Pause queue'));
  console.log(chalk.gray('  POST /api/queues/:name/resume - Resume queue'));
  console.log(chalk.gray('  GET  /api/events              - SSE stream'));
});

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log(chalk.yellow('\n[WARN] Shutting down dashboard server...'));

  // Close all queues
  await Promise.all(Object.values(queues).map((q) => q.close()));

  console.log(chalk.green('[OK] Dashboard server stopped.'));
  process.exit(0);
});
