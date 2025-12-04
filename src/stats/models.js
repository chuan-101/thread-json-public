import { tokenize } from './tokenize.js';

const MS_PER_DAY = 24 * 3600 * 1000;
const MODEL_STATE = new Map();

export function cutoff90Days(now = Date.now()) {
  // Rolling 90-day window (inclusive of the current moment)
  return now - 90 * MS_PER_DAY;
}

export function cutoff365Days(now = Date.now()) {
  // Rolling 12-month window (inclusive of the current moment)
  return now - 365 * MS_PER_DAY;
}

// Backwards-compatible alias (3 months ≈ 90 days)
export function threeMonthCutoff(now = Date.now()) {
  return cutoff90Days(now);
}

export function filterMessagesByWindow(messages, cutoff = cutoff90Days()) {
  if (!Array.isArray(messages)) {
    return [];
  }
  const numericCutoff = typeof cutoff === 'number' && Number.isFinite(cutoff) ? cutoff : cutoff90Days();
  return messages.filter((msg) => {
    const ts = typeof msg?.ts === 'number' && Number.isFinite(msg.ts) ? msg.ts : null;
    if (ts == null) {
      return false;
    }
    return ts >= numericCutoff;
  });
}

function approximateTokens(text) {
  if (!text) return 0;
  const tokens = tokenize(String(text));
  return tokens.length;
}

// Try multiple locations on the message to find the LLM model string
export function extractRawModel(msg) {
  if (!msg) return null;

  // Direct fields
  if (msg.model) return String(msg.model);

  // Common metadata locations
  if (msg.metadata?.model) return String(msg.metadata.model);
  if (msg.metadata?.default_model) return String(msg.metadata.default_model);
  if (msg.author_metadata?.model) return String(msg.author_metadata.model);
  if (msg.model_slug) return String(msg.model_slug);

  // TODO: If you see other fields in our JSON export that clearly hold
  // the LLM name, add them here.

  return null;
}

export function resetModelAgg() {
  MODEL_STATE.clear();
}

const UNKNOWN_RAW_MODELS = new Set();

export function resetUnknownRawModels() {
  UNKNOWN_RAW_MODELS.clear();
}

export function getUnknownRawModels() {
  return Array.from(UNKNOWN_RAW_MODELS.values());
}

function collectUnknownRawModel(raw) {
  if (!raw) return;
  UNKNOWN_RAW_MODELS.add(String(raw));
}

// ts: ms epoch, role: "assistant" | "user" | ...
export function bumpModelBucket(ts, role, msg) {
  if (role !== 'assistant') return; // model stats only for assistant

  const raw = extractRawModel(msg);
  const family = normModelFamily(raw);
  if (!family) {
    collectUnknownRawModel(raw); // for debug
    return; // ignore tools / unknowns
  }

  const timestamp = Number(ts);
  if (!Number.isFinite(timestamp)) return;

  const text = typeof msg?.text === 'string' ? msg.text : '';
  const msgLen = text.length;
  const tokenCount = approximateTokens(text);

  const yearMonth = toYearMonth(timestamp);
  const bucket = ensureModelBucket(yearMonth, timestamp);
  const modelEntry = bucket.models.get(family) || { msgs: 0, chars: 0, tokens: 0 };

  modelEntry.msgs += 1;
  bucket.totals.msgs += 1;

  const chars = Number(msgLen);
  if (Number.isFinite(chars) && chars > 0) {
    modelEntry.chars += chars;
    bucket.totals.chars += chars;
  }

  const tokens = Number(tokenCount);
  if (Number.isFinite(tokens) && tokens > 0) {
    modelEntry.tokens += tokens;
    bucket.totals.tokens += tokens;
  }

  bucket.models.set(family, modelEntry);
}

// options: { window: 'last12months' | 'all' }
export function getModelShare({ window = 'last12months', now, cutoff } = {}) {
  const timestampNow = Number.isFinite(now) ? now : Date.now();
  const rollingCutoff = cutoff == null ? (window === 'last12months' ? cutoff365Days(timestampNow) : 0) : cutoff;
  const numericCutoff = Number.isFinite(rollingCutoff) ? rollingCutoff : 0;

  if (window === 'last12months') {
    seedWindowBuckets(timestampNow, 12);
  }

  const counts = new Map();
  const sortedBuckets = Array.from(MODEL_STATE.values()).sort((a, b) => a.start - b.start);
  sortedBuckets.forEach((bucket) => {
    if (bucket.end < numericCutoff) return;
    for (const [family, metrics] of bucket.models.entries()) {
      const msgs = Number(metrics?.msgs) || 0;
      if (!msgs) continue;
      counts.set(family, (counts.get(family) || 0) + msgs);
    }
  });

  const rows = Array.from(counts.entries())
    .map(([family, count]) => ({ family, count }))
    .sort((a, b) => b.count - a.count || a.family.localeCompare(b.family));

  const total = rows.reduce((sum, r) => sum + r.count, 0);
  const divisor = total || 1;

  return {
    total,
    rows: rows.map((r) => ({
      id: r.family,
      label: r.family,
      messages: r.count,
      share: r.count / divisor,
    })),
  };
}

export function computeModelShare(messages, options = {}) {
  const now = typeof options?.now === 'number' && Number.isFinite(options.now) ? options.now : Date.now();
  const windowOpt = options?.window === 'all' ? 'all' : 'last12months';
  let cutoff;
  if (options && typeof options.cutoff === 'number' && Number.isFinite(options.cutoff)) {
    cutoff = options.cutoff;
  } else {
    cutoff = windowOpt === 'last12months' ? cutoff365Days(now) : 0;
  }

  resetModelAgg();
  const filtered = filterMessagesByWindow(messages, cutoff).filter((msg) => msg?.role === 'assistant');
  filtered.forEach((msg) => {
    bumpModelBucket(msg?.ts, msg?.role, msg);
  });

  const share = getModelShare({ window: windowOpt, now, cutoff });
  const buckets = buildBuckets(windowOpt, cutoff);

  return {
    total: share.total,
    rows: share.rows,
    entries: share.rows.map((entry) => ({ model: entry.label, value: entry.messages, share: entry.share })),
    buckets,
  };
}

function ensureModelBucket(key, ts) {
  let bucket = MODEL_STATE.get(key);
  if (bucket) return bucket;

  const { start, end } = monthBounds(ts);
  bucket = { key, start, end, totals: { msgs: 0, chars: 0, tokens: 0 }, models: new Map() };
  MODEL_STATE.set(key, bucket);
  return bucket;
}

function monthBounds(ts) {
  const date = new Date(ts);
  const startDate = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1, 0, 0, 0, 0);
  const endDate = Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 0, 23, 59, 59, 999);
  return { start: startDate, end: endDate };
}

function toYearMonth(ts) {
  const date = new Date(ts);
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  return `${year}-${month}`;
}

function seedWindowBuckets(now, months) {
  const anchor = new Date(now);
  anchor.setUTCDate(1);
  anchor.setUTCHours(0, 0, 0, 0);

  for (let i = 0; i < months; i += 1) {
    const d = new Date(anchor);
    d.setUTCMonth(anchor.getUTCMonth() - i, 1);
    const ts = d.getTime();
    const key = toYearMonth(ts);
    ensureModelBucket(key, ts);
  }
}

function buildBuckets(window, cutoff) {
  const numericCutoff = Number.isFinite(cutoff) ? cutoff : 0;
  const sortedBuckets = Array.from(MODEL_STATE.values()).sort((a, b) => a.start - b.start);
  return sortedBuckets
    .filter((bucket) => (window === 'last12months' ? bucket.end >= numericCutoff : true))
    .map((bucket) => {
      const models = Array.from(bucket.models.entries())
        .map(([family, metrics]) => ({ model: family, value: Number(metrics?.msgs) || 0 }))
        .filter((m) => m.value > 0)
        .sort((a, b) => b.value - a.value || a.model.localeCompare(b.model));

      const total = Number(bucket.totals?.msgs) || 0;
      return {
        key: bucket.key,
        start: bucket.start,
        end: bucket.end,
        total,
        models,
      };
    });
}

// Return a family label like "GPT-4o", "GPT-4.1", "GPT-3.5", "o1", "o3", etc.
// Return null for tools / unknowns.
export function normModelFamily(raw) {
  if (!raw) return null;
  const name = String(raw).toLowerCase().trim();

  // Exclude obvious tools
  if (name.startsWith('web.')) return null;
  if (name.startsWith('browser.')) return null;
  if (name.startsWith('python')) return null;
  if (name.includes('tool')) return null;

  // LLM patterns: anything starting with "gpt-" or "o1"/"o3"
  if (name.startsWith('gpt-')) {
    // Take first two segments as family, e.g.
    // "gpt-5.1-thinking" -> "GPT-5.1"
    // "gpt-4.1-mini"     -> "GPT-4.1"
    // "gpt-4o-mini"      -> "GPT-4o"
    const parts = name.split(/[-:]/); // split on "-" or ":"
    if (parts.length >= 2) {
      const base = parts[1]; // "5.1", "4.1", "4o", "3.5", etc.
      return `GPT-${base}`;
    }
    // fallback family
    return 'GPT (other)';
  }

  if (name.startsWith('o1')) return 'o1';
  if (name.startsWith('o3')) return 'o3';

  // If it looks like an "oX" LLM (e.g. "o4-mini"), we can treat "o4" as family:
  const match = /^o(\d+)/.exec(name);
  if (match) return `o${match[1]}`;

  // Everything else: treat as non-LLM (likely tools)
  return null;
}

function capitalize(input) {
  if (!input) return input;
  return input.charAt(0).toUpperCase() + input.slice(1);
}
