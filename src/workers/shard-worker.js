import { tokenize, inferTokenScript, FIXED_STOPWORDS } from '../stats/tokenize.js';
import { applyMask } from '../stats/mask.js';

const UNIGRAM_K = 1500;
const BIGRAM_K = 1500;
const TRIGRAM_K = 1000;
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

function collectAnnotatedTokens(tokens) {
  return tokens.map((token) => ({ token, script: inferTokenScript(token) }));
}

function collectNgrams(annotated, size) {
  if (size <= 1) {
    return annotated
      .map((entry) => entry.token)
      .filter((token) => token && !FIXED_STOPWORDS.has(token));
  }
  const grams = [];
  const window = [];
  let currentScript = null;
  for (const entry of annotated) {
    const token = entry.token;
    if (!token || FIXED_STOPWORDS.has(token)) {
      window.length = 0;
      currentScript = null;
      continue;
    }
    if (currentScript == null || entry.script !== currentScript) {
      window.length = 0;
      currentScript = entry.script;
    }
    window.push(token);
    if (window.length > size) {
      window.shift();
    }
    if (window.length === size) {
      grams.push(window.join(' '));
    }
  }
  return grams;
}

self.onmessage = async (event) => {
  const { shardId, text, messages, mask, cutoff } = event.data || {};
  try {
    const effectiveCutoff = typeof cutoff === 'number' && Number.isFinite(cutoff) ? cutoff : null;
    let sourceText = typeof text === 'string' ? text : '';
    if (Array.isArray(messages)) {
      const pieces = [];
      for (const msg of messages) {
        if (!msg) continue;
        const msgText = typeof msg.text === 'string' ? msg.text : '';
        if (!msgText) continue;
        const ts = typeof msg.ts === 'number' && Number.isFinite(msg.ts) ? msg.ts : null;
        if (effectiveCutoff != null && (ts == null || ts < effectiveCutoff)) {
          continue;
        }
        pieces.push(msgText);
      }
      sourceText = pieces.join('\n');
    }
    const masked = applyMask(sourceText, mask);
    const tokens = tokenize(masked);
    const annotated = collectAnnotatedTokens(tokens);
    const totalTokens = tokens.length;

    const unigrams = collectNgrams(annotated, 1);
    const bigrams = collectNgrams(annotated, 2);
    const trigrams = collectNgrams(annotated, 3);

    const uTopK = misraGries(unigrams, UNIGRAM_K);
    const bTopK = misraGries(bigrams, BIGRAM_K);
    const tTopK = misraGries(trigrams, TRIGRAM_K);

    const cmsU = buildCms(unigrams);
    const cmsB = buildCms(bigrams);
    const cmsT = buildCms(trigrams);

    self.postMessage({
      shardId,
      uTopK,
      bTopK,
      tTopK,
      cmsU: {
        depth: CMS_DEPTH,
        width: CMS_WIDTH,
        table: Array.from(cmsU),
      },
      cmsB: {
        depth: CMS_DEPTH,
        width: CMS_WIDTH,
        table: Array.from(cmsB),
      },
      cmsT: {
        depth: CMS_DEPTH,
        width: CMS_WIDTH,
        table: Array.from(cmsT),
      },
      totalTokens,
    });
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    self.postMessage({ shardId, error: { message: error.message } });
    throw error;
  }
};
