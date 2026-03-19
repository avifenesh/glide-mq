# Schedulers Reference

## Overview

`upsertJobScheduler` defines repeatable jobs via cron or fixed interval. Schedulers survive restarts - next run time is stored in Valkey.

## API

All scheduler operations are on the `Queue` instance:

```typescript
const queue = new Queue('tasks', { connection });
```

### Cron Schedule

```typescript
await queue.upsertJobScheduler(
  'daily-report',                    // scheduler ID (unique per queue)
  { pattern: '0 8 * * *' },         // cron expression
  { name: 'generate-report', data: { type: 'daily' } },  // job template
);
```

### Fixed Interval

```typescript
await queue.upsertJobScheduler(
  'cleanup',
  { every: 5 * 60 * 1_000 },        // interval in ms
  { name: 'cleanup-old', data: {} },
);
```

### Repeat After Complete

Schedules next job only after current completes (no overlap).

```typescript
await queue.upsertJobScheduler(
  'sensor-poll',
  { repeatAfterComplete: 5000 },     // 5s after previous completes
  { name: 'poll', data: { sensor: 'temp-1' } },
);
```

Mutually exclusive with `pattern` and `every`.

## Schedule Options

| Option | Type | Description |
|--------|------|-------------|
| `pattern` | `string` | Cron expression |
| `every` | `number` (ms) | Fixed interval |
| `repeatAfterComplete` | `number` (ms) | Interval after previous job completes |
| `startDate` | `Date \| number` | Defer first run until this time |
| `endDate` | `Date \| number` | Auto-remove scheduler when next run exceeds this |
| `limit` | `number` | Auto-remove after creating this many jobs |
| `tz` | `string` | IANA timezone for cron patterns (e.g., `'America/New_York'`) |

Only one of `pattern`, `every`, `repeatAfterComplete` per scheduler.

## Bounded Schedulers

```typescript
// Campaign window with max runs
await queue.upsertJobScheduler(
  'black-friday',
  {
    pattern: '0 */2 * * *',
    startDate: new Date('2026-11-28T00:00:00Z'),
    endDate: new Date('2026-12-01T00:00:00Z'),
    limit: 36,
  },
  { name: 'promote-deal', data: { campaign: 'bf' } },
);

// Interval with delayed start and hard stop
await queue.upsertJobScheduler(
  'warmup-cache',
  {
    every: 30_000,
    startDate: Date.now() + 60_000,
    endDate: new Date('2026-12-31'),
    limit: 100,
  },
  { name: 'warmup', data: { region: 'us-east' } },
);
```

## Management

```typescript
// List all schedulers
const schedulers = await queue.getRepeatableJobs();
// Returns stored bounds + iterationCount

// Get single scheduler details
const info = await queue.getJobScheduler('daily-report');

// Remove a scheduler (does not cancel in-flight jobs)
await queue.removeJobScheduler('cleanup');

// Upsert updates existing scheduler atomically
await queue.upsertJobScheduler('cleanup', { every: 10_000 }, { name: 'cleanup', data: {} });
```

## Gotchas

- `pattern`, `every`, `repeatAfterComplete` are mutually exclusive.
- `repeatAfterComplete` prevents overlap - next job only after current finishes or terminally fails.
- Scheduler ID is unique per queue. `upsert` replaces if exists.
- `removeJobScheduler` does not cancel jobs already in flight.
- Bounded options (`startDate`, `endDate`, `limit`) work with all three modes.
- Internal `Scheduler` class fires a promotion loop that converts due entries into real jobs.
- `getRepeatableJobs()` / `getJobScheduler()` expose `iterationCount` for inspection.
