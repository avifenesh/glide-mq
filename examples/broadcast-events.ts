/**
 * Broadcast Events - Event-driven AI notifications
 *
 * Real scenario: AI system publishes events (inference completed, failed,
 * billing usage) to a broadcast channel. Three BroadcastWorkers with
 * subject filters receive relevant events: a logger (all), a billing
 * service (billing only), and an alerts service (failures only).
 *
 * Run: npx tsx examples/broadcast-events.ts
 */
import { Broadcast, BroadcastWorker } from '../dist/index';
import { CONNECTION } from './llm';

const CHANNEL = `ai-events-${Date.now()}`;

async function main() {
  const broadcast = new Broadcast(CHANNEL, { connection: CONNECTION });

  const received: Record<string, string[]> = {
    logger: [],
    billing: [],
    alerts: [],
  };

  // Worker 1: Logger - receives ALL ai events
  const logger = new BroadcastWorker(CHANNEL, async (job) => {
    received.logger.push(job.data.subject);
    console.log(`  [logger]  ${job.data.subject}: ${JSON.stringify(job.data.payload)}`);
  }, {
    connection: CONNECTION,
    subscription: 'logger',
    subjects: ['ai.>'],
  });

  // Worker 2: Billing - only ai.billing.* events
  const billing = new BroadcastWorker(CHANNEL, async (job) => {
    received.billing.push(job.data.subject);
    console.log(`  [billing] ${job.data.subject}: cost=$${job.data.payload.cost}`);
  }, {
    connection: CONNECTION,
    subscription: 'billing',
    subjects: ['ai.billing.*'],
  });

  // Worker 3: Alerts - only ai.inference.failed events
  const alerts = new BroadcastWorker(CHANNEL, async (job) => {
    received.alerts.push(job.data.subject);
    console.log(`  [alerts]  ${job.data.subject}: ${job.data.payload.error}`);
  }, {
    connection: CONNECTION,
    subscription: 'alerts',
    subjects: ['ai.inference.failed'],
  });

  // Give workers time to initialize
  await new Promise(r => setTimeout(r, 1000));

  // Publish 5 events
  const events = [
    { subject: 'ai.inference.completed', payload: { model: 'gpt-4', tokens: 150, latencyMs: 800 } },
    { subject: 'ai.inference.completed', payload: { model: 'gpt-3.5', tokens: 80, latencyMs: 300 } },
    { subject: 'ai.inference.failed', payload: { model: 'gpt-4', error: 'Rate limit exceeded' } },
    { subject: 'ai.billing.usage', payload: { userId: 'user-42', cost: 0.015, tokens: 230 } },
    { subject: 'ai.inference.completed', payload: { model: 'gpt-4', tokens: 200, latencyMs: 1200 } },
  ];

  console.log(`Publishing ${events.length} events...\n`);
  for (const evt of events) {
    await broadcast.publish(evt.subject, { subject: evt.subject, payload: evt.payload });
  }

  // Wait for events to be processed
  const deadline = Date.now() + 15_000;
  const expectedLogger = 5;
  const expectedBilling = 1;
  const expectedAlerts = 1;

  while (Date.now() < deadline) {
    if (
      received.logger.length >= expectedLogger &&
      received.billing.length >= expectedBilling &&
      received.alerts.length >= expectedAlerts
    ) break;
    await new Promise(r => setTimeout(r, 300));
  }

  // Summary
  console.log('\n--- Event Distribution ---');
  console.log(`  logger (ai.>):              ${received.logger.length} event(s) - ${received.logger.join(', ')}`);
  console.log(`  billing (ai.billing.*):     ${received.billing.length} event(s) - ${received.billing.join(', ')}`);
  console.log(`  alerts (ai.inference.failed): ${received.alerts.length} event(s) - ${received.alerts.join(', ')}`);

  // Cleanup
  await logger.close(true);
  await billing.close(true);
  await alerts.close(true);
  await broadcast.close();

  console.log('\n[OK] Broadcast events example complete');
  process.exit(0);
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
