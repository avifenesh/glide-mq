import fs from 'fs';
import path from 'path';
import { describe, it, expect } from 'vitest';
import { Queue, Worker, Job, QueueEvents, FlowProducer } from '../src';
import { TestQueue, TestWorker } from '../src/testing';
import * as GlideMq from '../src';
import * as GlideMqTesting from '../src/testing';

const REPO_ROOT = path.resolve(__dirname, '..');
const DOC_FILES = [
  path.join(REPO_ROOT, 'README.md'),
  ...fs.readdirSync(path.join(REPO_ROOT, 'docs')).filter((f) => f.endsWith('.md')).map((f) => path.join(REPO_ROOT, 'docs', f)),
];

const CODE_BLOCK_REGEX = /```(?:ts|typescript|js|javascript)\n([\s\S]*?)```/g;
const IMPORT_REGEX = /^\s*import\s+(?:type\s+)?([\s\S]*?)\s+from\s+['"]([^'"]+)['"];?\s*$/gm;

const RECEIVER_PROTOTYPES: Record<string, object> = {
  queue: Queue.prototype,
  worker: Worker.prototype,
  job: Job.prototype,
  events: QueueEvents.prototype,
  qe: QueueEvents.prototype,
  flow: FlowProducer.prototype,
  flowProducer: FlowProducer.prototype,
  producer: FlowProducer.prototype,
  testQueue: TestQueue.prototype,
  testWorker: TestWorker.prototype,
};

function hasPrototypeMethod(proto: object, methodName: string): boolean {
  let current: object | null = proto;
  while (current && current !== Object.prototype) {
    if (Object.prototype.hasOwnProperty.call(current, methodName)) {
      return true;
    }
    current = Object.getPrototypeOf(current);
  }
  return false;
}

function extractNamedImports(clause: string): string[] {
  const named = clause.match(/\{([\s\S]*?)\}/);
  if (!named) return [];
  return named[1]
    .split(',')
    .map((token) => token.trim())
    .filter(Boolean)
    .map((token) => token.replace(/^type\s+/, ''))
    .map((token) => {
      const asMatch = token.match(/^([\w$]+)\s+as\s+([\w$]+)$/);
      return asMatch ? asMatch[1] : token;
    })
    .filter((token) => token !== 'type');
}

describe('Documentation examples', () => {
  it('import only public API symbols and call existing API methods', () => {
    const rootExports = new Set(Object.keys(GlideMq));
    const testingExports = new Set(Object.keys(GlideMqTesting));
    const errors: string[] = [];

    for (const filePath of DOC_FILES) {
      const markdown = fs.readFileSync(filePath, 'utf8');
      const fileLabel = path.relative(REPO_ROOT, filePath);
      let blockIndex = 0;
      let match: RegExpExecArray | null;

      while ((match = CODE_BLOCK_REGEX.exec(markdown)) !== null) {
        blockIndex += 1;
        const snippet = match[1];

        const imports = [...snippet.matchAll(IMPORT_REGEX)];
        const hasGlideImport = imports.some((m) => m[2] === 'glide-mq' || m[2] === 'glide-mq/testing');
        const hasNonGlideImport = imports.some((m) => m[2] !== 'glide-mq' && m[2] !== 'glide-mq/testing');

        for (const importMatch of imports) {
          const clause = importMatch[1];
          const moduleName = importMatch[2];
          const importedNames = extractNamedImports(clause);

          if (moduleName === 'glide-mq') {
            for (const name of importedNames) {
              if (!rootExports.has(name)) {
                errors.push(`${fileLabel}#${blockIndex}: '${name}' is not exported by glide-mq`);
              }
            }
          }

          if (moduleName === 'glide-mq/testing') {
            for (const name of importedNames) {
              if (!testingExports.has(name)) {
                errors.push(`${fileLabel}#${blockIndex}: '${name}' is not exported by glide-mq/testing`);
              }
            }
          }
        }

        // Skip method checks for non-glide-only snippets (e.g. BullMQ examples in migration docs)
        if (!hasGlideImport && hasNonGlideImport) {
          continue;
        }

        for (const [receiver, prototype] of Object.entries(RECEIVER_PROTOTYPES)) {
          const methodRegex = new RegExp(`\\b${receiver}\\.([A-Za-z_$][\\w$]*)\\s*\\(`, 'g');
          let methodMatch: RegExpExecArray | null;
          while ((methodMatch = methodRegex.exec(snippet)) !== null) {
            const methodName = methodMatch[1];
            if (!hasPrototypeMethod(prototype, methodName)) {
              errors.push(`${fileLabel}#${blockIndex}: '${receiver}.${methodName}()' is not part of the public API`);
            }
          }
        }
      }
    }

    expect(errors).toEqual([]);
  });
});
