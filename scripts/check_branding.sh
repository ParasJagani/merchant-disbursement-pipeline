#!/usr/bin/env bash
# Fails if any tracked file (or the git history) contains a denied term.
# Patterns come from .branding-denylist, or from $BRANDING_DENYLIST in CI.
set -euo pipefail
cd "$(dirname "$0")/.."

patterns_file="$(mktemp)"
trap 'rm -f "$patterns_file"' EXIT
if [[ -f .branding-denylist ]]; then
  grep -vE '^\s*(#|$)' .branding-denylist > "$patterns_file" || true
elif [[ -n "${BRANDING_DENYLIST:-}" ]]; then
  printf '%s\n' "$BRANDING_DENYLIST" | grep -vE '^\s*(#|$)' > "$patterns_file" || true
fi
if [[ ! -s "$patterns_file" ]]; then
  echo "branding check: no denylist configured, nothing to check (see .branding-denylist.example)"
  exit 0
fi

status=0
# Working tree (tracked + untracked, excluding ignored files and the denylist itself)
if git ls-files -z --cached --others --exclude-standard \
   | xargs -0 grep -nIiE -f "$patterns_file" -- 2>/dev/null; then
  echo "branding check: denied term found in the working tree" >&2
  status=1
fi
# History (commit messages and diffs)
if git log --all -p --format='%H %an %ae %s' 2>/dev/null | grep -niE -f "$patterns_file"; then
  echo "branding check: denied term found in git history" >&2
  status=1
fi
[[ $status -eq 0 ]] && echo "branding check: clean"
exit $status
