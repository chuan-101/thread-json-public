import { tokenize } from './tokenize.js';

const MS_PER_DAY = 24 * 3600 * 1000;

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

export function computeModelShare(messages, options = {}) {
  const now = typeof options?.now === 'number' && Number.isFinite(options.now) ? options.now : Date.now();
  const cutoff =
    options && typeof options.cutoff === 'number' && Number.isFinite(options.cutoff)
      ? options.cutoff
      : cutoff365Days(now);
  const metric = options?.metric;
  const filtered = filterMessagesByWindow(messages, cutoff).filter((msg) => msg?.role === 'assistant');
  if (!filtered.length) {
    return { total: 0, entries: [], buckets: [] };
  }

  // Cover the last 18 months to ensure we always have a 12-month window
  const monthSpan = Number.isFinite(options?.monthSpan) && options.monthSpan > 0 ? options.monthSpan : 18;
  const buckets = buildMonthlyBuckets(now, monthSpan);
  const bucketByKey = new Map(buckets.map((bucket) => [bucket.key, bucket]));

  const counts = new Map();
  filtered.forEach((msg) => {
    const model = typeof msg?.model === 'string' && msg.model ? msg.model : 'unknown';
    let value = 0;
    if (metric === 'chars') {
      value = (msg?.text || '').length;
    } else if (metric === 'tokens') {
      value = approximateTokens(msg?.text || '');
    } else {
      value = 1;
    }
    if (value > 0) {
      counts.set(model, (counts.get(model) || 0) + value);
    }

    const bucketKey = bucketKeyForTs(msg.ts, buckets);
    if (bucketKey) {
      const bucket = bucketByKey.get(bucketKey);
      if (bucket) {
        bucket.total += value;
        bucket.models.set(model, (bucket.models.get(model) || 0) + value);
      }
    }
  });

  const total = Array.from(counts.values()).reduce((sum, val) => sum + val, 0);
  const entries = Array.from(counts.entries())
    .map(([model, value]) => ({ model, value, share: total ? value / total : 0 }))
    .sort((a, b) => {
      if (b.value !== a.value) return b.value - a.value;
      return a.model.localeCompare(b.model);
    });

  const serializedBuckets = buckets
    .filter((bucket) => bucket.end >= cutoff)
    .map((bucket) => ({
      key: bucket.key,
      total: bucket.total,
      models: Array.from(bucket.models.entries()).map(([model, value]) => ({ model, value })),
    }));

  return { total, entries, buckets: serializedBuckets };
}

function buildMonthlyBuckets(now, monthSpan) {
  const buckets = [];
  const anchor = new Date(now);
  anchor.setUTCDate(1);
  anchor.setUTCHours(0, 0, 0, 0);

  for (let i = 0; i < monthSpan; i += 1) {
    const d = new Date(anchor);
    d.setUTCMonth(anchor.getUTCMonth() - i, 1);
    const start = d.getTime();
    const end = endOfMonth(start);
    buckets.push({
      key: `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`,
      start,
      end,
      total: 0,
      models: new Map(),
    });
  }
  // Ensure chronological order from oldest to newest
  return buckets.reverse();
}

function endOfMonth(startMs) {
  const d = new Date(startMs);
  d.setUTCMonth(d.getUTCMonth() + 1, 0);
  d.setUTCHours(23, 59, 59, 999);
  return d.getTime();
}

function bucketKeyForTs(ts, buckets) {
  if (!Number.isFinite(ts)) return null;
  for (const bucket of buckets) {
    if (ts >= bucket.start && ts <= bucket.end) {
      return bucket.key;
    }
  }
  return null;
}
