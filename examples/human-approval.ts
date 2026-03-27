/**
 * Human Approval - suspend / signal
 *
 * Real scenario: AI generates a customer email reply, human approves before sending.
 * The worker drafts an email, suspends for review, and resumes based on the
 * human's decision (approve or reject). On rejection, it generates a new draft.
 *
 * Run: npx tsx examples/human-approval.ts
 */
import readline from 'readline';
import { Queue, Worker } from '../dist/index';
import { chat, MODELS, CONNECTION, type Message } from './llm';

const QUEUE = `human-approval-${Date.now()}`;

async function main() {
  const queue = new Queue(QUEUE, { connection: CONNECTION });
  let completedResult: any = null;
  let failedReason: string | null = null;

  const worker = new Worker(QUEUE, async (job) => {
    const { complaint } = job.data;

    if (job.signals.length > 0) {
      const signal = job.signals[0];
      const signalData = typeof signal.data === 'string' ? JSON.parse(signal.data) : signal.data;

      if (signalData.action === 'approve') {
        console.log('\n[Worker] Approval received. Finalizing email...');
        return { status: 'sent', draft: signalData.draft };
      }

      if (signalData.action === 'reject') {
        console.log(`\n[Worker] Rejection received: "${signalData.reason}". Generating new draft...`);
        const messages: Message[] = [
          { role: 'system', content: 'You are a customer service agent. Write a polite email reply.' },
          { role: 'user', content: `Customer complaint: ${complaint}\nPrevious draft was rejected because: ${signalData.reason}\nWrite an improved reply (2-3 sentences).` },
        ];
        const result = await chat(MODELS.fast, messages, 150);
        console.log('\n--- Revised Draft ---');
        console.log(result.content);
        console.log('---\n');

        await job.suspend({ reason: 'awaiting-review-v2' });
        return; // unreachable - suspend throws
      }
    }

    // First invocation: generate initial draft
    console.log('[Worker] Generating email draft...');
    const messages: Message[] = [
      { role: 'system', content: 'You are a customer service agent. Write a polite email reply.' },
      { role: 'user', content: `Customer complaint: ${complaint}\nWrite a brief reply (2-3 sentences).` },
    ];
    const result = await chat(MODELS.fast, messages, 150);

    console.log('\n--- Draft Email ---');
    console.log(result.content);
    console.log('---\n');

    await job.suspend({
      reason: 'awaiting-review',
      onResume: async (signals) => {
        const sig = signals[0];
        const data = typeof sig.data === 'string' ? JSON.parse(sig.data) : sig.data;
        if (data.action === 'approve') {
          return { status: 'sent', draft: result.content };
        }
        // If rejected, the main processor handles re-generation above
        return undefined;
      },
    });
  }, { connection: CONNECTION, concurrency: 1 });

  worker.on('completed', (_job, result) => { completedResult = result; });
  worker.on('failed', (_job, err) => { failedReason = err.message; });

  const job = await queue.add('email-reply', {
    complaint: 'I ordered a laptop 2 weeks ago and it still has not arrived. Order #12345.',
  });

  if (!job) {
    console.error('Failed to add job');
    await cleanup(worker, queue);
    process.exit(1);
  }

  // Wait for the job to be suspended
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const info = await queue.getSuspendInfo(job.id);
    if (info) break;
    await new Promise(r => setTimeout(r, 300));
  }

  // Ask the human
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

  const answer = await new Promise<string>(resolve => {
    rl.question('Press Enter to approve, or type a reason to reject: ', resolve);
  });
  rl.close();

  const fetchedJob = await queue.getJob(job.id);
  const currentDraft = fetchedJob?.returnvalue ?? '(draft in worker memory)';

  if (answer.trim() === '') {
    console.log('Approving...');
    await queue.signal(job.id, 'review', { action: 'approve', draft: currentDraft });
  } else {
    console.log(`Rejecting with reason: "${answer.trim()}"`);
    await queue.signal(job.id, 'review', { action: 'reject', reason: answer.trim() });
  }

  // Wait for final completion
  while (!completedResult && !failedReason && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 300));
  }

  if (completedResult) {
    console.log(`\n[OK] Email ${completedResult.status}!`);
  } else if (failedReason) {
    console.log(`\n[ERROR] Job failed: ${failedReason}`);
  } else {
    console.log('\n[WARN] Timed out waiting for completion');
  }

  await cleanup(worker, queue);
  process.exit(0);
}

async function cleanup(worker: Worker, queue: Queue) {
  await worker.close(true);
  await queue.obliterate({ force: true });
  await queue.close();
}

main().catch((err) => {
  console.error('[ERROR]', err);
  process.exit(1);
});
