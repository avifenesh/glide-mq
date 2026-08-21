'use strict';

const { mkdirSync, readFileSync, writeFileSync } = require('node:fs');
const { dirname, join } = require('node:path');

const SRC = join(__dirname, '..', 'src', 'functions', 'glidemq.lua');
const DIST = join(__dirname, '..', 'dist', 'functions', 'glidemq.lua');
const EXECUTABLE = join(__dirname, '..', 'coverage', 'lua-executable.json');

const OPEN = new Set(['(', '{', '[']);
const CLOSE = new Set([')', '}', ']']);

function skipQuoted(line, start, quote) {
  for (let i = start; i < line.length; i++) {
    if (line[i] === '\\') {
      i++;
      continue;
    }
    if (line[i] === quote) return i;
  }
  return line.length;
}

function netDepth(line) {
  let depth = 0;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (c === '-' && line[i + 1] === '-') break;
    if (c === "'" || c === '"') {
      i = skipQuoted(line, i + 1, c);
      continue;
    }
    if (OPEN.has(c)) depth++;
    if (CLOSE.has(c)) depth--;
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

  out.push(
    lines[0] ?? '',
    'local __cov = {}',
    'local function __reg(name, fn)',
    '  redis.register_function(name, function(keys, args)',
    '    local ok, res = pcall(fn, keys, args)',
    '    if not ok then error(res) end',
    '    return res',
    '  end)',
    'end',
  );

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

  out.push(
    "__reg('glidemq_dumpCoverage', function(keys, args)",
    '  local hits = {}',
    "  for n, _ in pairs(__cov) do hits[#hits + 1] = tostring(n) end",
    '  table.sort(hits, function(a, b) return tonumber(a) < tonumber(b) end)',
    "  return table.concat(hits, ',')",
    'end)',
  );

  return { lua: out.join('\n'), executable };
}

function writeLcov(executable, hitSet, dest) {
  const lines = ['TN:', 'SF:src/functions/glidemq.lua'];
  let lh = 0;
  for (const n of executable) {
    const hit = hitSet.has(n) ? 1 : 0;
    if (hit) lh++;
    lines.push(`DA:${n},${hit}`);
  }
  lines.push(`LF:${executable.length}`, `LH:${lh}`, 'end_of_record');
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
}

if (require.main === module) main();

module.exports = { instrument, isSkippable, isContinuation, netDepth, writeLcov, EXECUTABLE };
