const fs = require('node:fs');
const path = require('node:path');

// Skills are installed independently: repository docs are not part of their tree.
function checkSkillLinks(skillsDir) {
  const findings = [];
  let files = 0;
  function visit(dir, skillRoot) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const file = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        visit(file, skillRoot);
      } else if (entry.isFile() && entry.name.endsWith('.md')) {
        files++;
        let fence;
        for (const [index, line] of fs.readFileSync(file, 'utf8').split('\n').entries()) {
          const marker = line.match(/^\s*(`{3,}|~{3,})/);
          if (marker) {
            if (!fence) fence = marker[1];
            else if (marker[1][0] === fence[0] && marker[1].length >= fence.length) fence = undefined;
            continue;
          }
          if (fence) continue;
          // Inline links/images and reference definitions used by the skill docs.
          const destinations = [...line.matchAll(/\]\(\s*<?([^\s)>]+)>?/g)].map((m) => m[1]);
          const definition = line.match(/^\s*\[[^\]]+\]:\s*<?([^\s>]+)>?/);
          if (definition) destinations.push(definition[1]);
          for (const destination of destinations) {
            if (/^(?:[a-z][a-z\d+.-]*:|#|\/\/)/i.test(destination)) continue;
            const target = path.resolve(path.dirname(file), decodeURI(destination.split(/[?#]/)[0]));
            const relative = path.relative(skillRoot, target);
            let reason;
            if (relative === '..' || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
              reason = 'link escapes the standalone skill tree';
            } else if (!fs.existsSync(target)) {
              reason = 'local link target does not exist';
            }
            if (reason) findings.push({ file, line: index + 1, destination, reason });
          }
        }
      }
    }
  }
  for (const entry of fs.readdirSync(skillsDir, { withFileTypes: true })) {
    const root = path.resolve(skillsDir, entry.name);
    if (entry.isDirectory() && fs.existsSync(path.join(root, 'SKILL.md'))) visit(root, root);
  }
  return { files, findings };
}

if (require.main === module) {
  const { files, findings } = checkSkillLinks(path.resolve(__dirname, '../skills'));
  for (const finding of findings) {
    console.error(`${finding.file}:${finding.line}: ${finding.reason}: ${finding.destination}`);
  }
  if (findings.length) process.exitCode = 1;
  else console.log(`Standalone skill links passed (${files} Markdown files).`);
}

module.exports = { checkSkillLinks };
