import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';

function readCiWorkflow(): string {
  const ref = process.env.CI_WORKFLOW_REF;
  if (ref) {
    return execFileSync('git', ['show', `${ref}:.github/workflows/ci.yml`], { encoding: 'utf8' });
  }
  return readFileSync(join(process.cwd(), '.github/workflows/ci.yml'), 'utf8');
}

describe('CI workflow', () => {
  it('passes the fuzzer exclusion to Vitest as one shell-quoted argument', () => {
    const workflow = readCiWorkflow();
    const integrationJob = workflow.slice(
      workflow.indexOf('  test-integration:'),
      workflow.indexOf('  test-lua-coverage:'),
    );

    expect(integrationJob).toContain("${{ matrix.node-version == '22' && '--exclude \"tests/fuzzer/**\"' || '' }}");
    expect(integrationJob).not.toContain("'--exclude tests/fuzzer/**'");
  });

  it('uses Codecov OIDC for coverage uploads', () => {
    const workflow = readCiWorkflow();
    const integrationJob = workflow.slice(
      workflow.indexOf('  test-integration:'),
      workflow.indexOf('  test-lua-coverage:'),
    );
    const luaCoverageJob = workflow.slice(workflow.indexOf('  test-lua-coverage:'), workflow.indexOf('  test-search:'));

    for (const job of [integrationJob, luaCoverageJob]) {
      expect(job).toContain('id-token: write');
      expect(job).toContain('use_oidc: true');
      expect(job).not.toContain('token: ${{ secrets.CODECOV_TOKEN }}');
    }
  });
});
