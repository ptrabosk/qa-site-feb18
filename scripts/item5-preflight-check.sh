#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

pass() { printf "PASS: %s\n" "$1"; }
fail() { printf "FAIL: %s\n" "$1"; exit 1; }

check_file_exists() {
    local path="$1"
    local label="$2"
    [[ -f "$path" ]] && pass "$label" || fail "$label (missing: $path)"
}

check_grep() {
    local pattern="$1"
    local path="$2"
    local label="$3"
    if rg -n --quiet "$pattern" "$path"; then
        pass "$label"
    else
        fail "$label"
    fi
}

check_file_exists "tools/uploader/pool-upload.gs" "Merged backend file exists"

if [[ -f "google-assignment-session-backend.gs" ]]; then
    fail "Legacy duplicate backend file still present (remove google-assignment-session-backend.gs)"
else
    pass "No duplicate backend file in repo"
fi

DOGET_COUNT="$(rg -n "^function doGet\\(e\\)" tools/uploader/pool-upload.gs | wc -l | tr -d ' ')"
DOPST_COUNT="$(rg -n "^function doPost\\(e\\)" tools/uploader/pool-upload.gs | wc -l | tr -d ' ')"
[[ "$DOGET_COUNT" == "1" ]] && pass "Single doGet in tools/uploader/pool-upload.gs" || fail "Unexpected doGet count: $DOGET_COUNT"
[[ "$DOPST_COUNT" == "1" ]] && pass "Single doPost in tools/uploader/pool-upload.gs" || fail "Unexpected doPost count: $DOPST_COUNT"

check_file_exists "qa-config.js" "Shared frontend config exists"
check_grep "window\\.QA_CONFIG" "qa-config.js" "qa-config.js exports window.QA_CONFIG"
check_grep "qa-config\\.js" "index.html" "index.html loads qa-config.js"
check_grep "qa-config\\.js" "app.html" "app.html loads qa-config.js"

check_grep "window\\.QA_CONFIG" "app.js" "app.js reads backend URL from shared config"
check_grep "window\\.QA_CONFIG" "login.js" "login.js reads backend URL from shared config"

PRIVATE_DOC_DIR=".private-docs"
BACKEND_SETUP_DOC="$PRIVATE_DOC_DIR/ASSIGNMENT_BACKEND_SETUP.md"
if [[ -f "$BACKEND_SETUP_DOC" ]]; then
    check_grep "ITEM5_DEPLOY_STAGING_PILOT_RUNBOOK\\.md" "$BACKEND_SETUP_DOC" "Backend setup links to Item 5 runbook"
else
    pass "Backend setup links to Item 5 runbook (skipped; optional private doc missing)"
fi

pass "Item 5 repo preflight checks complete"
