'use strict';

const { mkdirSync, readFileSync, writeFileSync } = require('node:fs');
const { dirname, join } = require('node:path');

const SRC = join(__dirname, '..', 'src', 'functions', 'glidemq.lua');
const DIST = join(__dirname, '..', 'dist', 'functions', 'glidemq.lua');
const DIST_INDEX = join(__dirname, '..', 'dist', 'functions', 'index.js');
const EXECUTABLE = join(__dirname, '..', 'coverage', 'lua-executable.json');
const VERSION_EXPORT = /exports\.LIBRARY_VERSION = '(\d+)'/;

function netDepth(line) {
  let depth = 0;
  let quote = null;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (quote) {
      if (c === '\\') {
        i++;
        continue;
      }
      if (c === quote) quote = null;
      continue;
    }
    if (c === '-' && line[i + 1] === '-') break;
    if (c === "'" || c === '"') {
      quote = c;
      continue;
    }
    if (c === '(' || c === '{' || c === '[') depth++;
    else if (c === ')' || c === '}' || c === ']') depth--;
  }
  return depth;
}

function isSkippable(trimmed) {
  return (
    trimmed === '' ||
    trimmed.startsWith('--') ||
    trimmed.startsWith('#!') ||
    trimmed === 'end' ||
    trimmed === 'end)' ||
    trimmed === 'else' ||
    trimmed.startsWith('elseif') ||
    trimmed === 'until' ||
    trimmed === ')' ||
    trimmed.startsWith(')')
  );
}

function isContinuation(trimmed) {
  if (!trimmed) return false;
  const c = trimmed[0];
  return c === "'" || c === '"' || c === ')' || c === '}' || c === ']';
}

const FUNCTION_TAIL = /\bfunction\s*\([^)]*\)\s*$/;
const INCOMPLETE_TAIL = /(?:\bor|\band|,|\(|\{)\s*$/;

function instrument(source) {
  const lines = source.split('\n');
  const executable = [];
  const out = [];
  let depth = 0;
  let incomplete = false;

  out.push(lines[0] ?? '');
  out.push('local __cov = {}');
  out.push('local function __reg(name, fn)');
  out.push('  redis.register_function(name, function(keys, args)');
  out.push('    local ok, res = pcall(fn, keys, args)');
  out.push('    if not ok then error(res) end');
  out.push('    return res');
  out.push('  end)');
  out.push('end');

  for (let i = 1; i < lines.length; i++) {
    const lineNo = i + 1;
    const line = lines[i].replaceAll('redis.register_function(', '__reg(');
    const trimmed = line.trim();
    const startDepth = depth;
    const skip = startDepth !== 0 || incomplete || isSkippable(trimmed) || isContinuation(trimmed);
    depth += netDepth(line);
    if (FUNCTION_TAIL.test(trimmed)) depth = 0;
    if (!isSkippable(trimmed)) incomplete = INCOMPLETE_TAIL.test(trimmed);
    if (skip) {
      out.push(line);
      continue;
    }
    executable.push(lineNo);
    const indent = line.match(/^\s*/)[0];
    out.push(`${indent}__cov[${lineNo}]=1; ${line.slice(indent.length)}`);
  }

  out.push("__reg('glidemq_dumpCoverage', function(keys, args)");
  out.push('  local hits = {}');
  out.push("  for n, _ in pairs(__cov) do hits[#hits + 1] = tostring(n) end");
  out.push('  table.sort(hits, function(a, b) return tonumber(a) < tonumber(b) end)');
  out.push("  return table.concat(hits, ',')");
  out.push('end)');

  return { lua: out.join('\n'), executable };
}

function stampCoverageVersion(js) {
  const next = js.replace(VERSION_EXPORT, (_, version) => `exports.LIBRARY_VERSION = '${version}-cov'`);
  if (next === js) {
    throw new Error('could not stamp coverage LIBRARY_VERSION in dist/functions/index.js');
  }
  return next;
}

function writeLcov(executable, hitSet, dest) {
  const lines = ['TN:', 'SF:src/functions/glidemq.lua'];
  let lh = 0;
  for (const n of executable) {
    const hit = hitSet.has(n) ? 1 : 0;
    if (hit) lh++;
    lines.push(`DA:${n},${hit}`);
  }
  lines.push(`LF:${executable.length}`);
  lines.push(`LH:${lh}`);
  lines.push('end_of_record');
  mkdirSync(dirname(dest), { recursive: true });
  writeFileSync(dest, `${lines.join('\n')}\n`);
}

function main() {
  if (process.env.GLIDEMQ_LUA_COVERAGE !== '1') return;
  const source = readFileSync(SRC, 'utf8');
  const { lua, executable } = instrument(source);
  mkdirSync(dirname(DIST), { recursive: true });
  writeFileSync(DIST, lua);
  mkdirSync(dirname(EXECUTABLE), { recursive: true });
  writeFileSync(EXECUTABLE, JSON.stringify(executable));
  writeFileSync(DIST_INDEX, stampCoverageVersion(readFileSync(DIST_INDEX, 'utf8')));
}

if (require.main === module) main();

module.exports = { instrument, isSkippable, isContinuation, netDepth, writeLcov, stampCoverageVersion, EXECUTABLE };
