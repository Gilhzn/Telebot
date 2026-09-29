#!/usr/bin/env bash
# Replace the "state" branch with the current state.json as a single commit.
# Used by .github/workflows/radar.yml; needs GH_TOKEN, GITHUB_REPOSITORY and RUNNER_TEMP.
set -euo pipefail
[ -f state.json ] || exit 0
dir="$RUNNER_TEMP/state-push"
rm -rf "$dir" && mkdir -p "$dir"
cp state.json "$dir/"
cd "$dir"
git init -q -b state
git config user.name radar-bot
git config user.email radar-bot@users.noreply.github.com
git add state.json
git commit -q -m "state $(date -u +%Y-%m-%dT%H:%M:%SZ)"
git push -q -f "https://x-access-token:${GH_TOKEN}@github.com/${GITHUB_REPOSITORY}.git" state
