# Audit Query Pipeline

This repo now includes a script that runs `queries/audits.sql` end-to-end, exports uploader-ready CSV, and resets Assignments from `cqa_target_sends`.

## Command

```bash
npm run audits:run
```

Optional flags:

```bash
node scripts/run-audits-pipeline.js --skip-sheet-update
node scripts/run-audits-pipeline.js --sql queries/audits.sql --out-dir queries/out
```

## Required env vars

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`

Optional:

- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_AUTHENTICATOR` (`externalbrowser` supported)
- `GOOGLE_SCRIPT_URL` (if omitted, script falls back to `qa-config.js`)

Auth note:

- If `SNOWFLAKE_AUTHENTICATOR=externalbrowser`, `SNOWFLAKE_PASSWORD` is not required.

## Output files

- `queries/out/audits-uploader.csv`
  - CSV structure aligned to the scenario uploader (`SEND_ID`, `COMPANY_NAME`, `CONVERSATION_JSON`, `ORDERS`, `HAS_SHOPIFY`, etc.)
- `queries/out/cqa-target-sends.csv`
  - distinct `send_id` list from `cqa_target_sends`

## Assignments reset behavior

The script calls Apps Script action `resetAssignmentsFromAudit` and:

1. clears `Assignments` sheet rows (keeps header),
2. inserts one row per `send_id` with status `AVAILABLE`,
3. clears/rebuilds `Pool` as `AVAILABLE` for those same `send_id`s.

If you use this action, deploy the latest `tools/uploader/pool-upload.gs` to your Apps Script Web App first.
