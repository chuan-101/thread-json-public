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
  if (!modelName) return;

  const timestamp = Number(ts);
  if (!Number.isFinite(timestamp)) return;

  const yearMonth = toYearMonth(timestamp);
  const bucket = ensureModelBucket(yearMonth, timestamp);
  const model = String(modelName);
  const modelEntry = bucket.models.get(model) || { msgs: 0, chars: 0, tokens: 0 };

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

  bucket.models.set(model, modelEntry);
}

// options: { window: 'last12months' | 'all', metric: 'msgs' | 'chars' | 'tokens', view: 'family' | 'model' }
export function getModelShare({ window = 'last12months', metric = 'chars', view = 'family', now, cutoff } = {}) {
  const timestampNow = Number.isFinite(now) ? now : Date.now();
  const rollingCutoff = cutoff == null ? (window === 'last12months' ? cutoff365Days(timestampNow) : 0) : cutoff;
  const numericCutoff = Number.isFinite(rollingCutoff) ? rollingCutoff : 0;

  if (window === 'last12months') {
    seedWindowBuckets(timestampNow, 12);
  }

  const sortedBuckets = Array.from(MODEL_STATE.values()).sort((a, b) => a.start - b.start);
  const totalsByModel = new Map();
  const buckets = [];

  sortedBuckets.forEach((bucket) => {
    if (bucket.end < numericCutoff) return;

    const bucketModels = new Map();
    for (const [model, metrics] of bucket.models.entries()) {
      const { id, label } = resolveModelView(model, view);
      const value = selectMetric(metrics, metric);
      if (!value) continue;
      bucketModels.set(id, (bucketModels.get(id) || { model: label, value: 0 }));
      bucketModels.get(id).value += value;
      totalsByModel.set(id, (totalsByModel.get(id) || { label, value: 0 }));
      totalsByModel.get(id).value += value;
    }

    const bucketTotal = selectMetric(bucket.totals, metric);
    buckets.push({
      key: bucket.key,
      start: bucket.start,
      end: bucket.end,
      total: bucketTotal,
      models: Array.from(bucketModels.values()).sort((a, b) => b.value - a.value || a.model.localeCompare(b.model)),
    });
  });

  const total = Array.from(totalsByModel.values()).reduce((sum, { value }) => sum + (Number(value) || 0), 0);
  const entries = Array.from(totalsByModel.entries())
    .map(([id, { label, value }]) => ({ id, label, value, pct: total ? value / total : 0 }))
    .sort((a, b) => {
      if (b.value !== a.value) return b.value - a.value;
      return a.label.localeCompare(b.label);
    });

  return { total, entries, buckets };
}

export function computeModelShare(messages, options = {}) {
  const now = typeof options?.now === 'number' && Number.isFinite(options.now) ? options.now : Date.now();
  const cutoff =
    options && typeof options.cutoff === 'number' && Number.isFinite(options.cutoff)
      ? options.cutoff
      : cutoff365Days(now);
  const metric = normalizeMetric(options?.metric);
  const view = options?.view === 'family' ? 'family' : 'model';

  resetModelAgg();
  const filtered = filterMessagesByWindow(messages, cutoff).filter((msg) => msg?.role === 'assistant');
  filtered.forEach((msg) => {
    const text = typeof msg?.text === 'string' ? msg.text : '';
    const chars = text.length;
    const tokens = approximateTokens(text);
    const model = typeof msg?.model === 'string' && msg.model ? msg.model : 'unknown';
    bumpModelBucket(msg?.ts, msg?.role, model, chars, tokens);
  });

    const share = getModelShare({ window: 'last12months', metric, view, now, cutoff });
    return {
      total: share.total,
      entries: share.entries.map((entry) => ({ model: entry.label, value: entry.value, share: entry.pct })),
      buckets: share.buckets.map((bucket) => ({
        key: bucket.key,
        start: bucket.start,
        end: bucket.end,
        total: bucket.total,
        models: bucket.models.map((m) => ({ model: m.model, value: m.value })),
      })),
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

function selectMetric(metrics, metric) {
  if (!metrics) return 0;
  if (metric === 'msgs') return Number(metrics.msgs) || 0;
  if (metric === 'tokens') return Number(metrics.tokens) || 0;
  return Number(metrics.chars) || 0;
}

function normalizeMetric(metric) {
  if (metric === 'msgs') return 'msgs';
  if (metric === 'tokens') return 'tokens';
  return 'chars';
}

function resolveModelView(model, view) {
  if (view !== 'family') {
    return { id: model, label: model };
  }
  const { id, label } = normModel(model);
  return { id, label };
}

function normModel(name) {
  const raw = typeof name === 'string' ? name.trim() : '';
  if (!raw) return { id: 'unknown', label: 'unknown' };
  const cleaned = raw.includes(':') ? raw.split(':').pop() : raw;
  const noVariant = cleaned.replace(/@(latest|stable)$/i, '');
  const stripDate = noVariant.replace(/-?20\d{2}-\d{2}-\d{2}.*/i, '');

  const families = [
    { match: /^gpt-4o-mini/i, id: 'gpt-4o', label: 'gpt-4o' },
    { match: /^gpt-4o/i, id: 'gpt-4o', label: 'gpt-4o' },
    { match: /^gpt-4\.1-mini/i, id: 'gpt-4.1', label: 'gpt-4.1' },
    { match: /^gpt-4\.1/i, id: 'gpt-4.1', label: 'gpt-4.1' },
    { match: /^gpt-3\.5/i, id: 'gpt-3.5', label: 'gpt-3.5' },
    { match: /^o3\b/i, id: 'o3', label: 'o3' },
  ];
  for (const family of families) {
    if (family.match.test(stripDate)) {
      return { id: family.id, label: family.label };
    }
  }
  const simplified = stripDate.replace(/-\d{4,}$/i, '');
  const normalized = simplified || stripDate || noVariant || cleaned;
  return { id: normalized, label: normalized };
}
