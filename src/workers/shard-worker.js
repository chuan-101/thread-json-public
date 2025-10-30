import { tokenize, createStopwordSet } from '../stats/tokenize.js';
import { applyMask } from '../stats/mask.js';

const MG_K = 2000;
const CMS_DEPTH = 4;
const CMS_WIDTH = 1 << 18; // 262144
const CMS_MASK = CMS_WIDTH - 1;
const CMS_SEEDS = [0x1b873593, 0xcc9e2d51, 0x9e3779b1, 0x85ebca6b];

function misraGries(tokens, k) {
  const counters = new Map();
  for (const token of tokens) {
    if (counters.has(token)) {
      counters.set(token, counters.get(token) + 1);
      continue;
    }
    if (counters.size < k) {
      counters.set(token, 1);
      continue;
    }
    for (const [key, value] of counters) {
      if (value <= 1) {
        counters.delete(key);
      } else {
        counters.set(key, value - 1);
      }
    }
  }

  if (!counters.size) {
    return [];
  }

  const tally = new Map();
  counters.forEach((_, key) => {
    tally.set(key, 0);
  });
  for (const token of tokens) {
    if (tally.has(token)) {
      tally.set(token, tally.get(token) + 1);
    }
  }
  const result = Array.from(tally.entries())
    .map(([token, count]) => ({ token, countEst: count }))
    .sort((a, b) => b.countEst - a.countEst);
  return result.slice(0, k);
}

function hashToken(token, seed) {
  let hash = seed >>> 0;
  for (let i = 0; i < token.length; i += 1) {
    hash = Math.imul(hash ^ token.charCodeAt(i), 0x5bd1e995);
    hash ^= hash >>> 13;
  }
  hash = Math.imul(hash ^ (hash >>> 15), 0x5bd1e995);
  return hash >>> 0;
}

function buildCms(tokens) {
  const table = new Uint32Array(CMS_DEPTH * CMS_WIDTH);
  for (const token of tokens) {
    for (let d = 0; d < CMS_DEPTH; d += 1) {
      const index = hashToken(token, CMS_SEEDS[d]) & CMS_MASK;
      const offset = d * CMS_WIDTH + index;
      table[offset] += 1;
    }
  }
  return table;
}

self.onmessage = async (event) => {
  const { shardId, text, stopwords, mask } = event.data || {};
  try {
    const masked = applyMask(typeof text === 'string' ? text : '', mask);
    const stopwordSet = createStopwordSet(stopwords);
    const tokens = tokenize(masked, stopwordSet);
    const totalTokens = tokens.length;
    const mgTopK = misraGries(tokens, MG_K);
    const cmsTable = buildCms(tokens);

    self.postMessage({
      shardId,
      mgTopK,
      cms: {
        depth: CMS_DEPTH,
        width: CMS_WIDTH,
        table: Array.from(cmsTable),
      },
      totalTokens,
    });
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    self.postMessage({ shardId, error: { message: error.message } });
    throw error;
  }
};
