import { tokenize } from './tokenize.js';

export function threeMonthCutoff(now = Date.now()) {
  const MS_PER_DAY = 24 * 3600 * 1000;
  // Rolling 90-day window (inclusive of the current moment)
  return now - 90 * MS_PER_DAY;
}

export function filterMessagesByWindow(messages, cutoff = threeMonthCutoff()) {
  if (!Array.isArray(messages)) {
    return [];
  }
  const numericCutoff = typeof cutoff === 'number' && Number.isFinite(cutoff) ? cutoff : threeMonthCutoff();
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
  const cutoff =
    options && typeof options.cutoff === 'number' && Number.isFinite(options.cutoff)
      ? options.cutoff
      : threeMonthCutoff();
  const metric = options?.metric;
  const filtered = filterMessagesByWindow(messages, cutoff);
  if (!filtered.length) {
    return { total: 0, entries: [] };
  }

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
  });

  const total = Array.from(counts.values()).reduce((sum, val) => sum + val, 0);
  const entries = Array.from(counts.entries())
    .map(([model, value]) => ({ model, value, share: total ? value / total : 0 }))
    .sort((a, b) => {
      if (b.value !== a.value) return b.value - a.value;
      return a.model.localeCompare(b.model);
    });

  return { total, entries };
}
