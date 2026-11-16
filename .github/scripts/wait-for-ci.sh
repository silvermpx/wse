#!/usr/bin/env bash
# Waits for the CI workflow's run of COMMIT to finish, and fails unless it
# passed: a release is made only of a commit CI passed. A run that passed
# once counts, re-runs included. GH_TOKEN reads the repository's runs.
#
#   .github/scripts/wait-for-ci.sh owner/repo COMMIT [MINUTES]
set -euo pipefail
repo="${1:?usage: wait-for-ci.sh REPO COMMIT [MINUTES]}"
commit="${2:?usage: wait-for-ci.sh REPO COMMIT [MINUTES]}"
minutes="${3:-120}"
deadline=$(( $(date +%s) + minutes * 60 ))
while :; do
  verdict="$(gh run list --repo "$repo" --workflow ci.yml --commit "$commit" --limit 20 \
    --json status,conclusion | python3 -c '
import json, sys
runs = json.load(sys.stdin)
if any(r["conclusion"] == "success" for r in runs):
    print("success")
elif not runs:
    print("none")
elif any(r["status"] != "completed" for r in runs):
    print("running")
else:
    print(runs[0]["conclusion"] or "unknown")
')"
  case "$verdict" in
    success)
      echo "CI passed for $commit"
      exit 0
      ;;
    none | running) ;;
    *)
      echo "::error::CI ended '$verdict' for $commit"
      exit 1
      ;;
  esac
  if [ "$(date +%s)" -ge "$deadline" ]; then
    echo "::error::CI did not pass for $commit within $minutes minutes (last: $verdict)"
    exit 1
  fi
  echo "CI for $commit: $verdict; asking again in a minute"
  sleep 60
done
