const fs = require('node:fs');
const path = require('node:path');

// Guard recurring incorrect claims, not every aspect of scheduling semantics.
const rules = [
  [/\b0(?:\s+\(the default\))?\s+(?:is|=)\s+(?:the\s+)?highest\b/i, '0 means no priority in glide-mq'],
  [/\ba lower number runs first\b/i, 'numeric priority does not guarantee execution order'],
  [
    /\bPriority and delayed jobs take precedence over LIFO\b/i,
    'only promoted non-LIFO priority-list retrieval has preference; delay has none',
  ],
  [
    /\bstrict reverse ordering is only guaranteed with\b/i,
    'single-worker concurrency does not guarantee global reverse execution order',
  ],
  [/\bDEL\s+glide:\{queueName\}:list-active\b/i, 'do not recommend deleting live list reservations'],
  [
    /\bnon-delayed priority jobs\b(?:\s+promote immediately\b|[^.\n]*\bso they promote immediately\b)/i,
    'priority jobs require scheduler promotion',
  ],
  [
    /\bpriority jobs are always (?:consumed|processed|fetched) first\b/i,
    'source preference is not unconditional execution order',
  ],
  [
    /(?:\b0(?:\s+\(default\))?\s*=\s*no priority,\s*|\bpriority 0\s+|\b0 means no priority and\s+)runs? after (?:every )?prioritized(?: jobs)?\b/i,
    'priority 0 has no global runs-after guarantee',
  ],
  [/\bpriority job starts no later than\b/i, 'a fetch check does not guarantee a particular job starts'],
  [/\bhigh-priority job\b.*\bruns after\b/i, 'FIFO list order does not guarantee execution order'],
  [/\blower number\s*=\s*higher priority\s*\(default:\s*0\)/i, 'identify 0 explicitly as no priority'],
  [/\badd\(['"]urgent['"].*\bpriority:\s*0\b/i, 'use priority 1 for an urgent priority example'],
  [/\blow priority\s*\(processed last\)/i, 'numeric priority does not guarantee last execution'],
  [
    /\bglide-mq runs them after any waiting job with priority > 0\b/i,
    'available-source preference does not guarantee execution order',
  ],
];

function findViolations(text) {
  const findings = [];
  for (const [index, line] of text.split('\n').entries()) {
    for (const [pattern, reason] of rules) {
      // Explicit comparisons may correctly describe BullMQ's different scheme.
      if (reason.startsWith('0 means') && /\bBullMQ\b/.test(line) && !/\bglide-mq\b/.test(line)) continue;
      if (pattern.test(line)) findings.push({ line: index + 1, reason });
    }
  }
  return findings;
}

function markdownFiles(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const file = path.join(dir, entry.name);
    if (entry.isDirectory()) return markdownFiles(file);
    return entry.isFile() && entry.name.endsWith('.md') ? [file] : [];
  });
}

if (require.main === module) {
  const root = path.resolve(__dirname, '..');
  // Historical changelog entries describe old behavior and are intentionally excluded.
  const files = ['README.md', 'HANDOVER.md']
    .map((file) => path.join(root, file))
    .concat(markdownFiles(path.join(root, 'docs')), markdownFiles(path.join(root, 'skills')));
  let count = 0;
  for (const file of files) {
    for (const finding of findViolations(fs.readFileSync(file, 'utf8'))) {
      console.error(`${path.relative(root, file)}:${finding.line}: ${finding.reason}`);
      count++;
    }
  }
  if (count) process.exitCode = 1;
  else console.log(`Priority documentation lint passed (${files.length} files).`);
}

module.exports = { findViolations };
