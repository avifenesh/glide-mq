#!/bin/bash
# Install git hooks for glide-mq
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOOKS_DIR="$(git -C "$SCRIPT_DIR" rev-parse --git-dir)/hooks"

cp "$SCRIPT_DIR/pre-push" "$HOOKS_DIR/pre-push"
chmod +x "$HOOKS_DIR/pre-push"

echo "[OK] Pre-push hook installed. Fuzzer will run before every push."
echo "     Bypass with: git push --no-verify"
