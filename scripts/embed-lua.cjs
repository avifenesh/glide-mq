'use strict';

const { readFileSync, writeFileSync } = require('node:fs');
const { join } = require('node:path');

const SRC = join(__dirname, '..', 'src', 'functions', 'glidemq.lua');
const OUT = join(__dirname, '..', 'src', 'functions', 'glidemq.embedded.json');

function embed(lua) {
  return `${JSON.stringify(lua)}\n`;
}

function main() {
  writeFileSync(OUT, embed(readFileSync(SRC, 'utf8')));
}

if (require.main === module) main();

module.exports = { embed, SRC, OUT };
