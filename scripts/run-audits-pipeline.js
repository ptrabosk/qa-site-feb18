#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const snowflake = require('snowflake-sdk');

const ROOT_DIR = path.resolve(__dirname, '..');
const DEFAULT_SQL_PATH = path.join(ROOT_DIR, 'queries', 'audits.sql');
const DEFAULT_OUTPUT_DIR = path.join(ROOT_DIR, 'queries', 'out');

function parseArgs(argv) {
  const out = {
    sql: DEFAULT_SQL_PATH,
    outDir: DEFAULT_OUTPUT_DIR,
    skipSheetUpdate: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = String(argv[i] || '');
    if (arg === '--skip-sheet-update') {
      out.skipSheetUpdate = true;
      continue;
    }
    if (arg === '--sql' && argv[i + 1]) {
      out.sql = path.resolve(String(argv[i + 1]));
      i += 1;
      continue;
    }
    if (arg === '--out-dir' && argv[i + 1]) {
      out.outDir = path.resolve(String(argv[i + 1]));
      i += 1;
      continue;
    }
  }

  return out;
}

function getEnv(name, fallback) {
  const value = process.env[name];
  if (value == null || String(value).trim() === '') return fallback;
  return String(value).trim();
}

function resolveGoogleScriptUrl() {
  const direct = getEnv('GOOGLE_SCRIPT_URL', '');
  if (direct) return direct;

  const qaConfigPath = path.join(ROOT_DIR, 'qa-config.js');
  if (!fs.existsSync(qaConfigPath)) return '';

  const text = fs.readFileSync(qaConfigPath, 'utf8');
  const match = text.match(/GOOGLE_SCRIPT_URL\s*:\s*'([^']+)'/);
  return match ? String(match[1]).trim() : '';
}

function getSnowflakeConfig() {
  const config = {
    account: getEnv('SNOWFLAKE_ACCOUNT', ''),
    username: getEnv('SNOWFLAKE_USER', ''),
    password: getEnv('SNOWFLAKE_PASSWORD', ''),
    role: getEnv('SNOWFLAKE_ROLE', ''),
    warehouse: getEnv('SNOWFLAKE_WAREHOUSE', ''),
    database: getEnv('SNOWFLAKE_DATABASE', ''),
    schema: getEnv('SNOWFLAKE_SCHEMA', ''),
    authenticator: getEnv('SNOWFLAKE_AUTHENTICATOR', ''),
  };

  const authenticator = String(config.authenticator || '').toLowerCase();
  const usesExternalBrowser = authenticator === 'externalbrowser';

  const required = {
    account: 'SNOWFLAKE_ACCOUNT',
    username: 'SNOWFLAKE_USER',
    warehouse: 'SNOWFLAKE_WAREHOUSE',
    database: 'SNOWFLAKE_DATABASE',
    schema: 'SNOWFLAKE_SCHEMA',
  };
  if (!usesExternalBrowser) {
    required.password = 'SNOWFLAKE_PASSWORD';
  }
  const missing = Object.keys(required).filter((key) => !config[key]);
  if (missing.length) {
    throw new Error(`Missing Snowflake env vars: ${missing.map((key) => required[key]).join(', ')}`);
  }

  if (!config.role) delete config.role;
  if (!config.authenticator) delete config.authenticator;
  if (usesExternalBrowser && !config.password) delete config.password;

  return config;
}

function splitSqlStatements(sqlText) {
  const statements = [];
  let buffer = '';
  let i = 0;
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inLineComment = false;
  let inBlockComment = false;

  while (i < sqlText.length) {
    const ch = sqlText[i];
    const next = i + 1 < sqlText.length ? sqlText[i + 1] : '';

    if (inLineComment) {
      buffer += ch;
      if (ch === '\n') inLineComment = false;
      i += 1;
      continue;
    }

    if (inBlockComment) {
      buffer += ch;
      if (ch === '*' && next === '/') {
        buffer += '/';
        i += 2;
        inBlockComment = false;
        continue;
      }
      i += 1;
      continue;
    }

    if (!inSingleQuote && !inDoubleQuote && ch === '-' && next === '-') {
      buffer += '--';
      i += 2;
      inLineComment = true;
      continue;
    }

    if (!inSingleQuote && !inDoubleQuote && ch === '/' && next === '*') {
      buffer += '/*';
      i += 2;
      inBlockComment = true;
      continue;
    }

    if (!inDoubleQuote && ch === "'") {
      if (inSingleQuote && next === "'") {
        buffer += "''";
        i += 2;
        continue;
      }
      inSingleQuote = !inSingleQuote;
      buffer += ch;
      i += 1;
      continue;
    }

    if (!inSingleQuote && ch === '"') {
      if (inDoubleQuote && next === '"') {
        buffer += '""';
        i += 2;
        continue;
      }
      inDoubleQuote = !inDoubleQuote;
      buffer += ch;
      i += 1;
      continue;
    }

    if (!inSingleQuote && !inDoubleQuote && ch === ';') {
      const statement = buffer.trim();
      if (statement) statements.push(statement);
      buffer = '';
      i += 1;
      continue;
    }

    buffer += ch;
    i += 1;
  }

  const tail = buffer.trim();
  if (tail) statements.push(tail);
  return statements;
}

function connectSnowflake(config) {
  const connection = snowflake.createConnection(config);
  const authenticator = String(config.authenticator || '').toLowerCase();
  const useAsyncConnect =
    authenticator === 'externalbrowser' ||
    authenticator.includes('okta');

  return new Promise((resolve, reject) => {
    if (useAsyncConnect && typeof connection.connectAsync === 'function') {
      connection
        .connectAsync((err, conn) => {
          if (err) {
            reject(err);
            return;
          }
          resolve(conn || connection);
        })
        .catch(reject);
      return;
    }

    connection.connect((err, conn) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(conn || connection);
    });
  });
}

function executeStatement(connection, sqlText) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
          return;
        }

        const safeRows = Array.isArray(rows) ? rows : [];
        resolve({
          statementId: stmt && stmt.getStatementId ? stmt.getStatementId() : '',
          queryId: stmt && stmt.getQueryId ? stmt.getQueryId() : '',
          rowCount: safeRows.length,
          rows: safeRows,
        });
      },
    });
  });
}

function destroyConnection(connection) {
  return new Promise((resolve) => {
    if (!connection) {
      resolve();
      return;
    }
    connection.destroy(() => resolve());
  });
}

function toJsonString(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

function toCsvCell(value) {
  const raw = value === null || value === undefined ? '' : String(value);
  if (/[",\n\r]/.test(raw)) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  return raw;
}

function writeCsv(filePath, headers, rows) {
  const lines = [];
  lines.push(headers.map(toCsvCell).join(','));

  for (let i = 0; i < rows.length; i += 1) {
    const row = rows[i];
    const line = headers
      .map((header) => {
        const value = Object.prototype.hasOwnProperty.call(row, header) ? row[header] : '';
        return toCsvCell(value);
      })
      .join(',');
    lines.push(line);
  }

  fs.writeFileSync(filePath, `${lines.join('\n')}\n`, 'utf8');
}

function mapFinalRowsToUploaderCsvRows(rows) {
  return rows.map((row) => ({
    SEND_ID: row.SEND_ID || row.send_id || '',
    COMPANY_NAME: row.COMPANY_NAME || row.company_name || '',
    COMPANY_WEBSITE: row.COMPANY_WEBSITE || row.company_website || '',
    PERSONA: row.PERSONA || row.persona || '',
    MESSAGE_TONE: row.MESSAGE_TONE || row.message_tone || '',
    CONVERSATION_JSON: toJsonString(row.CONVERSATION_JSON || row.conversation_json || []),
    LAST_5_PRODUCTS: toJsonString(row.LAST_5_PRODUCTS || row.last_5_products || []),
    ORDERS: toJsonString(row.ORDERS || row.orders || []),
    COMPANY_NOTES: row.COMPANY_NOTES || row.company_notes || '',
    ESCALATION_TOPICS: toJsonString(row.ESCALATION_TOPICS || row.escalation_topics || []),
    BLOCKLISTED_WORDS: toJsonString(row.BLOCKLISTED_WORDS || row.blocklisted_words || []),
    HAS_SHOPIFY: row.HAS_SHOPIFY != null ? String(row.HAS_SHOPIFY) : String(row.has_shopify || ''),
  }));
}

async function postResetAssignments(googleScriptUrl, sendIds) {
  const body = {
    action: 'resetAssignmentsFromAudit',
    send_ids: sendIds,
  };

  const response = await fetch(googleScriptUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'text/plain' },
    body: JSON.stringify(body),
  });

  const text = await response.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    throw new Error(`Apps Script returned non-JSON response: ${text.slice(0, 300)}`);
  }

  if (!response.ok || data.error) {
    throw new Error(data.error || `Assignments reset request failed with status ${response.status}`);
  }

  return data;
}

async function run() {
  const args = parseArgs(process.argv.slice(2));

  if (!fs.existsSync(args.sql)) {
    throw new Error(`SQL file not found: ${args.sql}`);
  }

  const sqlText = fs.readFileSync(args.sql, 'utf8');
  const statements = splitSqlStatements(sqlText);
  if (!statements.length) {
    throw new Error(`No SQL statements found in ${args.sql}`);
  }

  fs.mkdirSync(args.outDir, { recursive: true });

  const snowflakeConfig = getSnowflakeConfig();
  const googleScriptUrl = resolveGoogleScriptUrl();

  if (!args.skipSheetUpdate && !googleScriptUrl) {
    throw new Error('GOOGLE_SCRIPT_URL is not set and could not be read from qa-config.js');
  }

  console.log(`Running ${statements.length} SQL statements from ${path.relative(ROOT_DIR, args.sql)} ...`);

  const connection = await connectSnowflake(snowflakeConfig);
  let finalRows = [];

  try {
    for (let i = 0; i < statements.length; i += 1) {
      const statement = statements[i];
      const startedAt = Date.now();
      const preview = statement.replace(/\s+/g, ' ').slice(0, 120);
      console.log(`[${i + 1}/${statements.length}] ${preview}${preview.length >= 120 ? '...' : ''}`);
      const result = await executeStatement(connection, statement);
      const elapsedMs = Date.now() - startedAt;
      console.log(
        `  completed in ${(elapsedMs / 1000).toFixed(1)}s${result.queryId ? ` | queryId=${result.queryId}` : ''}`
      );
      if (i === statements.length - 1) {
        finalRows = result.rows;
      }
    }

    const targetSendResult = await executeStatement(
      connection,
      'SELECT DISTINCT send_id FROM cqa_target_sends ORDER BY send_id'
    );
    const targetSendIds = targetSendResult.rows
      .map((row) => String(row.SEND_ID || row.send_id || '').trim())
      .filter(Boolean);

    const uploaderRows = mapFinalRowsToUploaderCsvRows(finalRows);

    const uploaderCsvPath = path.join(args.outDir, 'audits-uploader.csv');
    const sendIdsCsvPath = path.join(args.outDir, 'cqa-target-sends.csv');

    writeCsv(
      uploaderCsvPath,
      [
        'SEND_ID',
        'COMPANY_NAME',
        'COMPANY_WEBSITE',
        'PERSONA',
        'MESSAGE_TONE',
        'CONVERSATION_JSON',
        'LAST_5_PRODUCTS',
        'ORDERS',
        'COMPANY_NOTES',
        'ESCALATION_TOPICS',
        'BLOCKLISTED_WORDS',
        'HAS_SHOPIFY',
      ],
      uploaderRows
    );

    writeCsv(
      sendIdsCsvPath,
      ['SEND_ID'],
      targetSendIds.map((sendId) => ({ SEND_ID: sendId }))
    );

    console.log(`\nWrote uploader CSV: ${path.relative(ROOT_DIR, uploaderCsvPath)} (${uploaderRows.length} rows)`);
    console.log(`Wrote target send IDs: ${path.relative(ROOT_DIR, sendIdsCsvPath)} (${targetSendIds.length} rows)`);

    if (!args.skipSheetUpdate) {
      console.log('Resetting assignments sheet from cqa_target_sends ...');
      const resetResult = await postResetAssignments(googleScriptUrl, targetSendIds);
      console.log(`Assignments reset complete: ${JSON.stringify(resetResult)}`);
    } else {
      console.log('Skipped assignments sheet update (--skip-sheet-update).');
    }
  } finally {
    await destroyConnection(connection);
  }
}

run().catch((error) => {
  console.error(`\nAudit pipeline failed: ${error.message || error}`);
  process.exit(1);
});
