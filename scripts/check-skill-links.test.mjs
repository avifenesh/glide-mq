import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'vitest';
import checker from './check-skill-links.cjs';

const { checkSkillLinks } = checker;

test('checks each standalone skill, retaining local sibling and reference links', () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'glide-mq-skill-links-'));
  try {
    const skills = path.join(root, 'skills');
    const skill = path.join(skills, 'example');
    fs.mkdirSync(path.join(skill, 'references'), { recursive: true });
    fs.mkdirSync(path.join(root, 'docs'));
    fs.writeFileSync(path.join(root, 'docs/DURABILITY.md'), '# Worker Crash Behavior');
    fs.writeFileSync(path.join(skill, 'SKILL.md'), '[Reference](references/one.md)\n[Section](#section)');
    fs.writeFileSync(path.join(skill, 'references/two.md'), '# Sibling');
    const reference = path.join(skill, 'references/one.md');
    fs.writeFileSync(
      reference,
      [
        '[Sibling](two.md#sibling)',
        '[Parent](../SKILL.md)',
        '[External](https://github.com/avifenesh/glide-mq/blob/main/docs/DURABILITY.md#worker-crash-behavior)',
        '[reference]: <two.md> "title"',
        '```md',
        '[Example](missing-example.md)',
        '```',
      ].join('\n'),
    );
    assert.deepEqual(checkSkillLinks(skills), { files: 3, findings: [] });
    fs.appendFileSync(
      reference,
      '\n[Crash](../../../docs/DURABILITY.md#worker-crash-behavior)\n[bad]: ../../../docs/DURABILITY.md\n[Missing](absent.md)',
    );
    const { findings } = checkSkillLinks(skills);
    assert.deepEqual(
      findings.map(({ line, reason }) => ({ line, reason })),
      [
        { line: 8, reason: 'link escapes the standalone skill tree' },
        { line: 9, reason: 'link escapes the standalone skill tree' },
        { line: 10, reason: 'local link target does not exist' },
      ],
    );
    // A neighboring skill must not accidentally satisfy an installed skill's link.
    fs.mkdirSync(path.join(skills, 'neighbor'));
    fs.writeFileSync(path.join(skills, 'neighbor/SKILL.md'), '[Other](../example/SKILL.md)');
    assert.equal(checkSkillLinks(skills).findings.length, 4);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});
