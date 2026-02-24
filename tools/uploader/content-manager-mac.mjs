#!/usr/bin/env node

import fs from 'fs';
import path from 'path';
import readline from 'readline/promises';
import { stdin as input, stdout as output } from 'process';
import { spawnSync } from 'child_process';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function ensureDirectory(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function readJson(filePath, fallback = {}) {
  if (!fs.existsSync(filePath)) return fallback;
  const raw = fs.readFileSync(filePath, 'utf8').trim();
  if (!raw) return fallback;
  return JSON.parse(raw);
}

function writeJson(filePath, value) {
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function getStringValue(value) {
  if (value === null || value === undefined) return '';
  return String(value).normalize('NFKC');
}

function normalizeColumnKey(name) {
  return getStringValue(name).trim().toLowerCase().replace(/[^a-z0-9]/g, '');
}

function getRowValue(row, candidates) {
  if (!row || typeof row !== 'object') return '';
  const entries = Object.entries(row);
  for (const key of candidates) {
    if (Object.prototype.hasOwnProperty.call(row, key)) {
      const value = getStringValue(row[key]);
      if (value.trim()) return value;
    }
  }

  for (const key of candidates) {
    const normalizedCandidate = normalizeColumnKey(key);
    for (const [rowKey, rowValue] of entries) {
      if (normalizeColumnKey(rowKey) === normalizedCandidate) {
        const value = getStringValue(rowValue);
        if (value.trim()) return value;
      }
    }
  }

  return '';
}

function parseJsonText(text) {
  const raw = getStringValue(text).trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function toArray(value) {
  if (value === null || value === undefined) return [];
  if (Array.isArray(value)) return value;
  if (typeof value === 'object') return [value];
  return [];
}

function convertScenarioContainerToList(container) {
  if (!container) return [];
  if (Array.isArray(container)) return container;
  if (Array.isArray(container.scenarios)) return container.scenarios;
  if (container.scenarios && typeof container.scenarios === 'object') {
    return Object.values(container.scenarios);
  }
  return [];
}

function getTemplateListFromContainer(container) {
  if (!container) return [];
  if (Array.isArray(container)) return container;
  if (Array.isArray(container.templates)) return container.templates;
  return [];
}

function uniqueTrimmedStrings(value) {
  const out = [];
  const seen = new Set();
  const values = Array.isArray(value) ? value : [value];
  for (const item of values) {
    if (item === null || item === undefined) continue;
    const trimmed = getStringValue(item).trim();
    if (!trimmed || trimmed === '{}' || trimmed === '[]') continue;
    const key = trimmed.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(trimmed);
  }
  return out;
}

function parseListLikeText(text) {
  const raw = getStringValue(text).trim();
  if (!raw || raw === '[]') return [];

  const parsed = parseJsonText(raw);
  if (parsed !== null) {
    if (Array.isArray(parsed)) return uniqueTrimmedStrings(parsed);
    if (typeof parsed === 'object') return uniqueTrimmedStrings(Object.values(parsed));
    return uniqueTrimmedStrings(parsed);
  }

  const stripped = raw.replace(/^\[/, '').replace(/\]$/, '');
  return uniqueTrimmedStrings(
    stripped
      .split(/[\n\r,]+/)
      .map((part) => part.trim().replace(/^['"]|['"]$/g, ''))
      .filter(Boolean)
  );
}

function normalizeMessageType(typeRaw, agentId) {
  const value = getStringValue(typeRaw).trim().toLowerCase();
  if (!value) return agentId ? 'agent' : 'customer';
  if (['agent', 'assistant', 'support', 'csr', 'rep', 'outbound', 'outgoing'].includes(value)) {
    return 'agent';
  }
  if (['system', 'automation', 'bot'].includes(value)) return 'system';
  if (['inbound', 'incoming', 'customer', 'user', 'client'].includes(value)) return 'customer';
  return value;
}

function normalizeMessageMedia(media) {
  if (media === null || media === undefined) return [];
  const items = Array.isArray(media) ? media : [media];
  const urls = [];
  for (const item of items) {
    const raw = getStringValue(item).trim();
    if (!raw) continue;
    const parsed = parseJsonText(raw);
    if (Array.isArray(parsed)) {
      for (const parsedItem of parsed) {
        const maybe = getStringValue(parsedItem).trim();
        if (/^https?:\/\//i.test(maybe)) urls.push(maybe);
      }
      continue;
    }
    const matches = raw.match(/https?:\/\/[^\s<>"']+/gi);
    if (matches) urls.push(...matches.map((u) => u.trim()));
    else if (/^https?:\/\//i.test(raw)) urls.push(raw);
  }
  return urls;
}

function normalizeCompanyKey(name) {
  return getStringValue(name).trim().toLowerCase().replace(/\s+/g, ' ');
}

function slugFromCompanyKey(companyKey) {
  const slug = getStringValue(companyKey).toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
  return slug || 'company';
}

function parseBool(value) {
  const raw = getStringValue(value).trim().toLowerCase();
  if (!raw) return false;
  return ['true', '1', 'yes', 'y'].includes(raw);
}

function parseCompanyNotesToCategories(notesText) {
  const text = getStringValue(notesText).trim();
  if (!text) return {};

  const notes = {};
  let currentKey = 'important';
  notes[currentKey] = [];

  const lines = text.split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) continue;
    if (line.startsWith('#')) {
      const heading = line.replace(/^#+/, '').trim().toLowerCase();
      const normalized = heading.replace(/&/g, 'and').replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
      currentKey = normalized || 'important';
      if (!notes[currentKey]) notes[currentKey] = [];
      continue;
    }

    let item = line;
    if (item.startsWith('•') || item.startsWith('-')) item = item.slice(1).trim();
    if (item) notes[currentKey].push(item);
  }

  const clean = {};
  for (const [key, items] of Object.entries(notes)) {
    const unique = uniqueTrimmedStrings(items);
    if (unique.length) clean[key] = unique;
  }
  return clean;
}

function normalizeScenarioRecordForStorage(scenario) {
  const out = scenario && typeof scenario === 'object' ? { ...scenario } : {};

  const rightPanel = out.rightPanel && typeof out.rightPanel === 'object' ? { ...out.rightPanel } : {};
  if (out.source !== undefined && rightPanel.source === undefined) {
    rightPanel.source = out.source;
    delete out.source;
  }
  if (out.browsingHistory !== undefined && rightPanel.browsingHistory === undefined) {
    rightPanel.browsingHistory = out.browsingHistory;
    delete out.browsingHistory;
  }
  if (out.browsing_history !== undefined && rightPanel.browsingHistory === undefined) {
    rightPanel.browsingHistory = out.browsing_history;
    delete out.browsing_history;
  }
  if (out.orders !== undefined && rightPanel.orders === undefined) {
    rightPanel.orders = out.orders;
    delete out.orders;
  }
  if (out.templatesUsed !== undefined && rightPanel.templates === undefined) {
    rightPanel.templates = out.templatesUsed;
    delete out.templatesUsed;
  }
  if (Object.keys(rightPanel).length > 0) out.rightPanel = rightPanel;

  out.blocklisted_words = uniqueTrimmedStrings(out.blocklisted_words ?? out.blocklistedWords ?? []);
  delete out.blocklistedWords;

  out.escalation_preferences = uniqueTrimmedStrings(out.escalation_preferences ?? out.escalationPreferences ?? []);
  delete out.escalationPreferences;

  if (out.notes === undefined && out.guidelines !== undefined) out.notes = out.guidelines;
  if (!out.notes || typeof out.notes !== 'object') out.notes = {};
  delete out.guidelines;

  return out;
}

function mergeScenariosById(existing, incoming) {
  const result = existing.map((item) => normalizeScenarioRecordForStorage(item));
  const idToIndex = new Map();

  result.forEach((item, index) => {
    const id = getStringValue(item.id).trim();
    if (id && !idToIndex.has(id)) idToIndex.set(id, index);
  });

  let added = 0;
  let updated = 0;

  for (const item of incoming) {
    const normalizedIncoming = normalizeScenarioRecordForStorage(item);
    const incomingId = getStringValue(normalizedIncoming.id).trim();
    if (incomingId && idToIndex.has(incomingId)) {
      const targetIndex = idToIndex.get(incomingId);
      const base = result[targetIndex] || {};
      const merged = { ...base, ...normalizedIncoming };
      if (base.rightPanel || normalizedIncoming.rightPanel) {
        merged.rightPanel = { ...(base.rightPanel || {}), ...(normalizedIncoming.rightPanel || {}) };
      }
      result[targetIndex] = normalizeScenarioRecordForStorage(merged);
      updated += 1;
      continue;
    }

    result.push(normalizedIncoming);
    added += 1;
    if (incomingId) idToIndex.set(incomingId, result.length - 1);
  }

  return { scenarios: result, added, updated };
}

function normalizeTemplateRecordForStorage(template) {
  const src = template && typeof template === 'object' ? template : {};
  const out = {
    name: getStringValue(src.name).trim(),
    content: getStringValue(src.content).trim(),
  };
  const id = getStringValue(src.id).trim();
  const shortcut = getStringValue(src.shortcut).trim();
  const companyName = getStringValue(src.companyName ?? src.company_name ?? src.company).trim();
  if (id) out.id = id;
  if (shortcut) out.shortcut = shortcut;
  if (companyName) out.companyName = companyName;
  return out;
}

function mergeTemplates(existing, incoming) {
  const result = existing.map((item) => normalizeTemplateRecordForStorage(item));
  const byId = new Map();
  const byComposite = new Map();

  const compositeKey = (tpl) => `${getStringValue(tpl.companyName).trim().toLowerCase()}|${getStringValue(tpl.name).trim().toLowerCase()}`;

  result.forEach((tpl, index) => {
    const id = getStringValue(tpl.id).trim();
    if (id && !byId.has(id)) byId.set(id, index);
    byComposite.set(compositeKey(tpl), index);
  });

  let added = 0;
  let updated = 0;

  for (const incomingTemplate of incoming) {
    const tpl = normalizeTemplateRecordForStorage(incomingTemplate);
    if (!tpl.name || !tpl.content) continue;

    const id = getStringValue(tpl.id).trim();
    const comp = compositeKey(tpl);
    let targetIndex = -1;

    if (id && byId.has(id)) targetIndex = byId.get(id);
    else if (byComposite.has(comp)) targetIndex = byComposite.get(comp);

    if (targetIndex >= 0) {
      result[targetIndex] = { ...result[targetIndex], ...tpl };
      updated += 1;
      continue;
    }

    result.push(tpl);
    const newIndex = result.length - 1;
    if (id) byId.set(id, newIndex);
    byComposite.set(comp, newIndex);
    added += 1;
  }

  return { templates: result, added, updated };
}

function parseCsv(text) {
  const rows = [];
  const data = text.replace(/^\uFEFF/, '');
  const allRows = [];
  let current = [];
  let field = '';
  let inQuotes = false;

  for (let i = 0; i < data.length; i += 1) {
    const ch = data[i];
    const next = i + 1 < data.length ? data[i + 1] : '';

    if (inQuotes) {
      if (ch === '"' && next === '"') {
        field += '"';
        i += 1;
      } else if (ch === '"') {
        inQuotes = false;
      } else {
        field += ch;
      }
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
    } else if (ch === ',') {
      current.push(field);
      field = '';
    } else if (ch === '\n') {
      current.push(field);
      allRows.push(current);
      current = [];
      field = '';
    } else if (ch === '\r') {
      continue;
    } else {
      field += ch;
    }
  }

  if (field.length > 0 || current.length > 0) {
    current.push(field);
    allRows.push(current);
  }

  if (!allRows.length) return rows;
  const headers = allRows[0].map((h) => h.trim());

  for (let i = 1; i < allRows.length; i += 1) {
    const values = allRows[i];
    if (!values || values.every((v) => !getStringValue(v).trim())) continue;
    const row = {};
    for (let j = 0; j < headers.length; j += 1) {
      row[headers[j]] = values[j] ?? '';
    }
    rows.push(row);
  }

  return rows;
}

function convertCsvRowToScenario(row) {
  const conversationRaw = getRowValue(row, ['CONVERSATION_JSON', 'CONVERSATION', 'MESSAGES_JSON', 'MESSAGES']);
  const conversationItems = toArray(parseJsonText(conversationRaw));
  const conversation = [];

  for (const msg of conversationItems) {
    if (!msg || typeof msg !== 'object') continue;
    const agentId = getStringValue(msg.agent ?? msg.agent_id ?? msg.agentId ?? msg.agentID).trim();
    const typeRaw = msg.message_type ?? msg.type ?? msg.role ?? msg.direction ?? msg.sender ?? msg.speaker;
    const entry = {
      message_media: normalizeMessageMedia(msg.message_media ?? msg.media ?? msg.attachments),
      message_text: getStringValue(msg.message_text ?? msg.text ?? msg.content ?? msg.body),
      message_type: normalizeMessageType(typeRaw, agentId),
    };
    if (agentId) entry.agent = agentId;
    const messageId = getStringValue(msg.message_id ?? msg.id).trim();
    if (messageId) entry.message_id = messageId;
    const dateTime = getStringValue(msg.date_time ?? msg.created_at ?? msg.timestamp).trim();
    if (dateTime) entry.date_time = dateTime;
    if (getStringValue(entry.message_text).trim()) conversation.push(entry);
  }

  const browsingHistoryRaw = getRowValue(row, [
    'LAST_5_PRODUCTS',
    'LAST5_PRODUCTS',
    'BROWSING_HISTORY',
    'RECENT_PRODUCTS',
  ]);
  const browsingHistoryItems = toArray(parseJsonText(browsingHistoryRaw));
  const browsingHistory = [];
  for (const p of browsingHistoryItems) {
    if (!p || typeof p !== 'object') continue;
    const name = getStringValue(p.product_name ?? p.name ?? p.product ?? p.title).trim();
    const link = getStringValue(p.product_link ?? p.link ?? p.url).trim();
    const viewDate = getStringValue(p.view_date ?? p.last_viewed ?? p.time_ago ?? p.viewed_at).trim();
    if (!name && !link) continue;
    const item = { item: name || link };
    if (link) item.link = link;
    if (viewDate) item.timeAgo = viewDate;
    browsingHistory.push(item);
  }

  const ordersRaw = getRowValue(row, ['ORDERS', 'ORDER_HISTORY', 'PAST_ORDERS']);
  const orderItems = toArray(parseJsonText(ordersRaw));
  const orders = [];

  for (const order of orderItems) {
    if (!order || typeof order !== 'object') continue;
    const productsForOrder = toArray(order.products ?? order.items ?? order.line_items);
    const items = [];
    for (const prod of productsForOrder) {
      if (!prod || typeof prod !== 'object') continue;
      const product = {
        name: getStringValue(prod.product_name ?? prod.name ?? prod.product ?? prod.title).trim(),
      };
      const price = prod.product_price ?? prod.price ?? prod.unit_price;
      const productLink = getStringValue(prod.product_link ?? prod.link ?? prod.url).trim();
      if (price !== undefined && getStringValue(price).trim()) product.price = price;
      if (productLink) product.productLink = productLink;
      if (product.name || product.price !== undefined || product.productLink) items.push(product);
    }

    const orderOut = {
      orderNumber: getStringValue(order.order_number ?? order.order_id ?? order.number).trim(),
      orderDate: getStringValue(order.order_date ?? order.date ?? order.created_at).trim(),
      items,
    };

    const orderLink = getStringValue(
      order.order_status_url ?? order.order_status_link ?? order.link ?? order.status_url ?? order.status_link
    ).trim();
    const trackingLink = getStringValue(
      order.order_tracking_link ?? order.tracking_link ?? order.tracking_url ?? order.order_tracking_url
    ).trim();

    if (orderLink) orderOut.link = orderLink;
    if (trackingLink) {
      orderOut.trackingLink = trackingLink;
      orderOut.order_tracking_link = trackingLink;
    }

    const passthroughFields = ['total', 'discount', 'coupon', 'created', 'created_at', 'date_time', 'email'];
    for (const field of passthroughFields) {
      if (order[field] !== undefined && getStringValue(order[field]).trim()) {
        orderOut[field] = order[field];
      }
    }

    if (
      orderOut.orderNumber ||
      orderOut.orderDate ||
      orderOut.items.length ||
      orderOut.link ||
      orderOut.trackingLink ||
      orderOut.total ||
      orderOut.discount ||
      orderOut.coupon
    ) {
      orders.push(orderOut);
    }
  }

  const companyWebsite = getRowValue(row, ['COMPANY_WEBSITE', 'WEBSITE', 'SITE_URL']).trim();
  const rightPanel = {
    source: {
      label: 'Website',
      value: companyWebsite,
      date: '',
    },
  };
  if (browsingHistory.length) rightPanel.browsingHistory = browsingHistory;
  if (orders.length) rightPanel.orders = orders;

  const notesText = getRowValue(row, ['COMPANY_NOTES', 'NOTES', 'GUIDELINES', 'INTERNAL_NOTES']);

  const scenario = {
    id: getRowValue(row, ['SEND_ID', 'SCENARIO_ID', 'ID']).trim(),
    companyName: getRowValue(row, ['COMPANY_NAME', 'BRAND', 'COMPANY']).trim(),
    companyWebsite,
    agentName: getRowValue(row, ['PERSONA', 'AGENT_NAME', 'AGENT']).trim(),
    messageTone: getRowValue(row, ['MESSAGE_TONE', 'TONE']).trim(),
    conversation,
    notes: parseCompanyNotesToCategories(notesText),
    rightPanel,
    escalation_preferences: parseListLikeText(
      getRowValue(row, ['ESCALATION_TOPICS', 'ESCALATION_PREFERENCES', 'ESCALATIONS'])
    ),
    blocklisted_words: parseListLikeText(
      getRowValue(row, ['BLOCKLISTED_WORDS', 'BLOCKLIST_WORDS', 'BLOCKLIST', 'BLOCKED_WORDS'])
    ),
  };

  const hasShopifyRaw = getRowValue(row, ['HAS_SHOPIFY', 'SHOPIFY', 'HAS_SHOPIFY_STORE']);
  if (hasShopifyRaw.trim()) scenario.has_shopify = parseBool(hasShopifyRaw);

  return scenario;
}

function convertCsvRowToTemplate(row) {
  const template = {
    name: getRowValue(row, ['NAME', 'TEMPLATE_NAME', 'TITLE']).trim(),
    content: getRowValue(row, ['CONTENT', 'TEMPLATE_CONTENT', 'BODY']).trim(),
  };

  const id = getRowValue(row, ['ID', 'TEMPLATE_ID']).trim();
  const shortcut = getRowValue(row, ['SHORTCUT']).trim();
  const companyName = getRowValue(row, ['COMPANY_NAME', 'COMPANY', 'BRAND']).trim();

  if (id) template.id = id;
  if (shortcut) template.shortcut = shortcut;
  if (companyName) template.companyName = companyName;

  return template;
}

function buildRuntimeArtifacts(currentFolder) {
  const dataRoot = path.join(currentFolder, 'data');
  const scenarioRoot = path.join(dataRoot, 'scenarios');
  const scenarioChunksRoot = path.join(scenarioRoot, 'chunks');
  const templateRoot = path.join(dataRoot, 'templates');
  const templateCompaniesRoot = path.join(templateRoot, 'companies');

  ensureDirectory(dataRoot);
  ensureDirectory(scenarioRoot);
  ensureDirectory(scenarioChunksRoot);
  ensureDirectory(templateRoot);
  ensureDirectory(templateCompaniesRoot);

  const chunkSize = 5;

  const scenarioContainer = readJson(path.join(currentFolder, 'scenarios.json'), { scenarios: [] });
  const scenarioList = convertScenarioContainerToList(scenarioContainer).map((s) => normalizeScenarioRecordForStorage(s));

  const scenarioOrder = [];
  const scenarioByKey = {};
  const scenarioById = {};
  const scenarioChunkBuckets = new Map();

  for (let i = 0; i < scenarioList.length; i += 1) {
    const key = String(i + 1);
    const record = scenarioList[i];
    const chunkNumber = Math.floor(i / chunkSize) + 1;
    const chunkBase = `chunk_${String(chunkNumber).padStart(4, '0')}`;
    const chunkFileName = `${chunkBase}.json`;

    if (!scenarioChunkBuckets.has(chunkFileName)) scenarioChunkBuckets.set(chunkFileName, {});
    scenarioChunkBuckets.get(chunkFileName)[key] = record;

    scenarioOrder.push(key);
    const scenarioId = getStringValue(record.id).trim();
    const companyName = getStringValue(record.companyName).trim();
    scenarioByKey[key] = {
      id: scenarioId,
      companyName,
      chunkFile: `data/scenarios/chunks/${chunkFileName}`,
    };
    if (scenarioId && !scenarioById[scenarioId]) scenarioById[scenarioId] = key;
  }

  const keepChunkFiles = new Set();
  for (const [chunkFileName, scenarios] of scenarioChunkBuckets.entries()) {
    const chunkPath = path.join(scenarioChunksRoot, chunkFileName);
    writeJson(chunkPath, {
      version: 1,
      chunk: chunkFileName.replace(/\.json$/i, ''),
      scenarios,
    });
    keepChunkFiles.add(chunkFileName);
  }

  if (fs.existsSync(scenarioChunksRoot)) {
    for (const file of fs.readdirSync(scenarioChunksRoot)) {
      if (file.endsWith('.json') && !keepChunkFiles.has(file)) {
        fs.unlinkSync(path.join(scenarioChunksRoot, file));
      }
    }
  }

  writeJson(path.join(scenarioRoot, 'index.json'), {
    version: 1,
    chunkSize,
    order: scenarioOrder,
    byKey: scenarioByKey,
    byId: scenarioById,
  });

  const templatesContainer = readJson(path.join(currentFolder, 'templates.json'), { templates: [] });
  const templateList = getTemplateListFromContainer(templatesContainer).map((tpl) =>
    normalizeTemplateRecordForStorage(tpl)
  );

  const scenarioCompanyKeys = new Set();
  for (const scenario of scenarioList) {
    const companyKey = normalizeCompanyKey(scenario.companyName);
    if (companyKey) scenarioCompanyKeys.add(companyKey);
  }

  const globalTemplates = [];
  const templatesByCompany = new Map();

  for (const template of templateList) {
    const companyKey = normalizeCompanyKey(template.companyName);
    if (!companyKey) {
      globalTemplates.push(template);
      continue;
    }
    if (!scenarioCompanyKeys.has(companyKey)) continue;
    if (!templatesByCompany.has(companyKey)) templatesByCompany.set(companyKey, []);
    templatesByCompany.get(companyKey).push(template);
  }

  writeJson(path.join(templateRoot, 'global.json'), { templates: globalTemplates });

  const companiesIndex = {};
  const usedSlugs = new Set();
  const keepTemplateFiles = new Set();

  const companyKeys = Array.from(templatesByCompany.keys()).sort();
  for (const companyKey of companyKeys) {
    const base = slugFromCompanyKey(companyKey);
    let slug = base;
    let suffix = 2;
    while (usedSlugs.has(slug)) {
      slug = `${base}-${suffix}`;
      suffix += 1;
    }
    usedSlugs.add(slug);

    const fileName = `${slug}.json`;
    writeJson(path.join(templateCompaniesRoot, fileName), {
      companyKey,
      templates: templatesByCompany.get(companyKey),
    });

    companiesIndex[companyKey] = `data/templates/companies/${fileName}`;
    keepTemplateFiles.add(fileName);
  }

  if (fs.existsSync(templateCompaniesRoot)) {
    for (const file of fs.readdirSync(templateCompaniesRoot)) {
      if (file.endsWith('.json') && !keepTemplateFiles.has(file)) {
        fs.unlinkSync(path.join(templateCompaniesRoot, file));
      }
    }
  }

  writeJson(path.join(templateRoot, 'index.json'), {
    version: 1,
    globalFile: 'data/templates/global.json',
    companies: companiesIndex,
  });

  return {
    scenarios: scenarioList.length,
    scenarioChunks: scenarioChunkBuckets.size,
    templates: templateList.length,
    templateCompanies: companyKeys.length,
  };
}

function getScenarioCount(currentFolder) {
  const data = readJson(path.join(currentFolder, 'scenarios.json'), { scenarios: [] });
  return convertScenarioContainerToList(data).length;
}

function getTemplateCount(currentFolder) {
  const data = readJson(path.join(currentFolder, 'templates.json'), { templates: [] });
  return getTemplateListFromContainer(data).length;
}

function detectCurrentFolder() {
  const candidates = [__dirname, path.dirname(__dirname), path.dirname(path.dirname(__dirname))];
  for (const candidate of candidates) {
    const scenariosPath = path.join(candidate, 'scenarios.json');
    const templatesPath = path.join(candidate, 'templates.json');
    if (fs.existsSync(scenariosPath) && fs.existsSync(templatesPath)) return candidate;
  }
  return process.cwd();
}

async function askPath(rl, prompt) {
  const answer = (await rl.question(prompt)).trim();
  if (!answer) return '';
  return path.isAbsolute(answer) ? answer : path.resolve(process.cwd(), answer);
}

function ensureRequiredFiles(currentFolder) {
  const scenariosPath = path.join(currentFolder, 'scenarios.json');
  const templatesPath = path.join(currentFolder, 'templates.json');
  if (!fs.existsSync(scenariosPath)) writeJson(scenariosPath, { scenarios: [] });
  if (!fs.existsSync(templatesPath)) writeJson(templatesPath, { templates: [] });
}

async function main() {
  let currentFolder = detectCurrentFolder();
  ensureRequiredFiles(currentFolder);

  const rl = readline.createInterface({ input, output });

  console.log('Scenario & Template Manager (macOS CLI)');
  console.log('----------------------------------------');

  try {
    while (true) {
      const scenarioCount = getScenarioCount(currentFolder);
      const templateCount = getTemplateCount(currentFolder);

      console.log(`\nCurrent folder: ${currentFolder}`);
      console.log(`Scenarios: ${scenarioCount} | Templates: ${templateCount}`);
      console.log('1) Upload scenarios (JSON/CSV)');
      console.log('2) Upload templates (JSON/CSV)');
      console.log('3) Clear scenarios');
      console.log('4) Clear templates');
      console.log('5) Rebuild runtime artifacts');
      console.log('6) Choose different folder');
      console.log('7) Open current folder in Finder');
      console.log('8) Exit');

      const choice = (await rl.question('Select an option: ')).trim();

      if (choice === '1') {
        const sourcePath = await askPath(rl, 'Path to scenarios JSON/CSV: ');
        if (!sourcePath || !fs.existsSync(sourcePath)) {
          console.log('File not found.');
          continue;
        }

        const ext = path.extname(sourcePath).toLowerCase();
        const existing = convertScenarioContainerToList(
          readJson(path.join(currentFolder, 'scenarios.json'), { scenarios: [] })
        );

        let incoming = [];
        if (ext === '.csv') {
          const csvRows = parseCsv(fs.readFileSync(sourcePath, 'utf8'));
          incoming = csvRows.map(convertCsvRowToScenario).filter((s) => s.id && s.companyName);
          if (!incoming.length) {
            console.log('No valid scenarios found in CSV (requires SEND_ID and COMPANY_NAME).');
            continue;
          }
        } else {
          const parsed = readJson(sourcePath, null);
          incoming = convertScenarioContainerToList(parsed);
          if (!incoming.length) {
            console.log('No scenarios found in JSON file.');
            continue;
          }
        }

        const merged = mergeScenariosById(existing, incoming);
        writeJson(path.join(currentFolder, 'scenarios.json'), { scenarios: merged.scenarios });
        const artifacts = buildRuntimeArtifacts(currentFolder);
        console.log(
          `scenarios.json updated. Added: ${merged.added}, Updated: ${merged.updated}. Runtime refreshed (${artifacts.scenarioChunks} chunks).`
        );
        continue;
      }

      if (choice === '2') {
        const sourcePath = await askPath(rl, 'Path to templates JSON/CSV: ');
        if (!sourcePath || !fs.existsSync(sourcePath)) {
          console.log('File not found.');
          continue;
        }

        const ext = path.extname(sourcePath).toLowerCase();
        const existing = getTemplateListFromContainer(
          readJson(path.join(currentFolder, 'templates.json'), { templates: [] })
        );

        let incoming = [];
        if (ext === '.csv') {
          const csvRows = parseCsv(fs.readFileSync(sourcePath, 'utf8'));
          incoming = csvRows.map(convertCsvRowToTemplate).filter((t) => t.name && t.content);
        } else {
          const parsed = readJson(sourcePath, null);
          incoming = getTemplateListFromContainer(parsed);
        }

        if (!incoming.length) {
          console.log('No templates found in source file.');
          continue;
        }

        const merged = mergeTemplates(existing, incoming);
        writeJson(path.join(currentFolder, 'templates.json'), { templates: merged.templates });
        const artifacts = buildRuntimeArtifacts(currentFolder);
        console.log(
          `templates.json updated. Added: ${merged.added}, Updated: ${merged.updated}. Runtime refreshed (${artifacts.templateCompanies} company bundles).`
        );
        continue;
      }

      if (choice === '3') {
        writeJson(path.join(currentFolder, 'scenarios.json'), { scenarios: [] });
        const artifacts = buildRuntimeArtifacts(currentFolder);
        console.log(`scenarios.json cleared. Runtime refreshed (${artifacts.scenarioChunks} chunks).`);
        continue;
      }

      if (choice === '4') {
        writeJson(path.join(currentFolder, 'templates.json'), { templates: [] });
        const artifacts = buildRuntimeArtifacts(currentFolder);
        console.log(`templates.json cleared. Runtime refreshed (${artifacts.templateCompanies} company bundles).`);
        continue;
      }

      if (choice === '5') {
        const artifacts = buildRuntimeArtifacts(currentFolder);
        console.log(
          `Runtime artifacts built. Scenarios: ${artifacts.scenarios}, Chunks: ${artifacts.scenarioChunks}, Templates: ${artifacts.templates}, Template companies: ${artifacts.templateCompanies}.`
        );
        continue;
      }

      if (choice === '6') {
        const folderPath = await askPath(rl, 'Folder path (must contain scenarios.json + templates.json): ');
        if (!folderPath || !fs.existsSync(folderPath) || !fs.statSync(folderPath).isDirectory()) {
          console.log('Invalid folder path.');
          continue;
        }

        const scenariosPath = path.join(folderPath, 'scenarios.json');
        const templatesPath = path.join(folderPath, 'templates.json');
        if (!fs.existsSync(scenariosPath) || !fs.existsSync(templatesPath)) {
          console.log('Folder must contain scenarios.json and templates.json.');
          continue;
        }

        currentFolder = folderPath;
        console.log(`Folder changed to ${currentFolder}`);
        continue;
      }

      if (choice === '7') {
        const result = spawnSync('open', [currentFolder], { stdio: 'ignore' });
        if (result.status !== 0) console.log('Failed to open folder in Finder.');
        else console.log('Opened folder in Finder.');
        continue;
      }

      if (choice === '8') {
        console.log('Done.');
        break;
      }

      console.log('Invalid option.');
    }
  } catch (error) {
    console.error(`Error: ${error.message}`);
  } finally {
    rl.close();
  }
}

main();
