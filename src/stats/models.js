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

export function resetModelAgg() {
  MODEL_STATE.clear();
}

// ts: ms epoch, role: "assistant" | "user" | ...
export function bumpModelBucket(ts, role, modelName, msgLen, tokenCount) {
  if (role !== 'assistant') return; // model stats only for assistant
  const family = normModelFamily(modelName);
  if (!family) return; // ignore tools / unknowns

  const timestamp = Number(ts);
  if (!Number.isFinite(timestamp)) return;

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
    const text = typeof msg?.text === 'string' ? msg.text : '';
    const chars = text.length;
    const tokens = approximateTokens(text);
    const model = typeof msg?.model === 'string' && msg.model ? msg.model : 'unknown';
    bumpModelBucket(msg?.ts, msg?.role, model, chars, tokens);
  });

  const share = getModelShare({ window: windowOpt, now, cutoff });
  const buckets = buildBuckets(windowOpt, cutoff);

  return {
    total: share.total,
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

// normalize raw model names to a small set of primary families
export function normModelFamily(raw) {
  if (!raw) return null;
  const name = String(raw).toLowerCase();

  // GPT-4o family
  if (name.startsWith('gpt-4o')) return 'GPT-4o';

  // GPT-4.1 family
  if (name.startsWith('gpt-4.1')) return 'GPT-4.1';

  // GPT-4.5 / 4.x variants (optional)
  if (name.startsWith('gpt-4.5')) return 'GPT-4.5';
  if (name.startsWith('gpt-4')) return 'GPT-4 (other)';

  // GPT-3.5 family
  if (name.startsWith('gpt-3.5')) return 'GPT-3.5';

  // o3 / o1 families (if present)
  if (name.startsWith('o3')) return 'o3';
  if (name.startsWith('o1')) return 'o1';

  // tools and unknown: treat as non-LLM
  return null;
}
