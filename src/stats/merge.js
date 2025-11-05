import { WorkerPool } from '../workers/pool.js';
import { WHITELIST } from './tokenize.js';

const CMS_SEEDS = [0x1b873593, 0xcc9e2d51, 0x9e3779b1, 0x85ebca6b];
const RECOUNT_MAX_CHUNK_CHARS = 4_000_000; // ~8MB of UTF-16 text
const DEFAULT_LIMIT = 3000;
const MAX_UNION_CANDIDATES = 3000;

const DEFAULT_SCORING = {
  alpha: 0.3,
  beta: 0.2,
  gamma: 1.15,
  delta: 0.25,
  minFreq: 5,
  minPMI: 1.5,
};

const WHITELIST_BONUS = 1.1;

function hashToken(token, seed) {
  let hash = seed >>> 0;
  for (let i = 0; i < token.length; i += 1) {
    hash = Math.imul(hash ^ token.charCodeAt(i), 0x5bd1e995);
    hash ^= hash >>> 13;
  }
  hash = Math.imul(hash ^ (hash >>> 15), 0x5bd1e995);
  return hash >>> 0;
}

function cmsEstimate(token, cms) {
  if (!cms || !cms.table) {
    return 0;
  }
  const { depth, width, table } = cms;
  if (!depth || !width) {
    return 0;
  }
  const isArrayLike = Array.isArray(table) || ArrayBuffer.isView(table);
  if (!isArrayLike) {
    return 0;
  }
  const counts = [];
  for (let d = 0; d < depth && d < CMS_SEEDS.length; d += 1) {
    const seed = CMS_SEEDS[d % CMS_SEEDS.length];
    const index = width && (width & (width - 1)) === 0
      ? hashToken(token, seed) & (width - 1)
      : hashToken(token, seed) % width;
    const offset = d * width + index;
    const value = Number(table[offset]) || 0;
    counts.push(value);
  }
  if (!counts.length) return 0;
  return counts.reduce((min, value) => (value < min ? value : min), counts[0]);
}

function splitTokens(phrase) {
  if (typeof phrase !== 'string') return [];
  return phrase
    .split(/\s+/)
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function ngramLength(phrase) {
  const parts = splitTokens(phrase);
  return parts.length || 1;
}

function isWhitelistedPhrase(parts) {
  if (!Array.isArray(parts) || parts.length <= 1) {
    return false;
  }
  return parts.every((part) => WHITELIST.has(part));
}

function appendOrdered(set, list, token) {
  if (!token) return;
  if (set.has(token)) return;
  set.add(token);
  list.push(token);
}

function aggregateCounts(store, token, delta) {
  if (!token) return;
  const next = (store.get(token) || 0) + delta;
  store.set(token, next);
}

function orderCandidates(entries, cmsList, limit) {
  if (!Array.isArray(entries) || !entries.length) {
    return [];
  }
  const enriched = entries.map(([token, mgCount]) => ({
    token,
    mgCount: Number.isFinite(mgCount) ? mgCount : 0,
  }));
  enriched.forEach((entry) => {
    if (!cmsList?.length) {
      entry.upperBound = entry.mgCount;
      return;
    }
    let upperBound = 0;
    for (const cms of cmsList) {
      upperBound += cmsEstimate(entry.token, cms);
    }
    entry.upperBound = upperBound || entry.mgCount;
  });
  enriched.sort((a, b) => {
    if (b.upperBound !== a.upperBound) {
      return b.upperBound - a.upperBound;
    }
    if (b.mgCount !== a.mgCount) {
      return b.mgCount - a.mgCount;
    }
    return a.token.localeCompare(b.token);
  });
  const sliceEnd = limit > 0 ? Math.min(limit, enriched.length) : enriched.length;
  return enriched.slice(0, sliceEnd).map((item) => item.token);
}

export function mergePartials(partials, options = {}) {
  const opts = typeof options === 'number' ? { K: options } : options || {};
  if (!Array.isArray(partials) || !partials.length) {
    return {
      candidates: [],
      candidatesByN: { 1: [], 2: [], 3: [] },
      totalTokens: 0,
    };
  }

  const defaultLimit = Number.isFinite(opts.K) ? opts.K : DEFAULT_LIMIT;
  const limitPerN = {
    1: defaultLimit,
    2: defaultLimit,
    3: defaultLimit,
  };
  if (opts.limitPerN) {
    for (const n of [1, 2, 3]) {
      const value = opts.limitPerN[n];
      if (Number.isFinite(value) && value > 0) {
        limitPerN[n] = value;
      }
    }
  }

  const mgCounts = {
    1: new Map(),
    2: new Map(),
    3: new Map(),
  };
  const cmsByN = {
    1: [],
    2: [],
    3: [],
  };
  let totalTokens = 0;

  const configs = [
    { n: 1, topKey: 'uTopK', cmsKey: 'cmsU' },
    { n: 2, topKey: 'bTopK', cmsKey: 'cmsB' },
    { n: 3, topKey: 'tTopK', cmsKey: 'cmsT' },
  ];

  partials.forEach((partial) => {
    if (!partial) return;
    const shardTokens = Number(partial.totalTokens);
    if (Number.isFinite(shardTokens)) {
      totalTokens += shardTokens;
    }

    if (Array.isArray(partial.mgTopK)) {
      partial.mgTopK.forEach((entry) => {
        const token = entry && typeof entry.token === 'string' ? entry.token : null;
        if (!token) return;
        const countEst = Number(entry.countEst) || 0;
        aggregateCounts(mgCounts[1], token, countEst);
      });
    }
    if (partial.cms) {
      cmsByN[1].push(partial.cms);
    }

    configs.forEach(({ n, topKey, cmsKey }) => {
      const top = Array.isArray(partial[topKey]) ? partial[topKey] : [];
      top.forEach((entry) => {
        const token = entry && typeof entry.token === 'string' ? entry.token.trim() : '';
        if (!token) return;
        const countEst = Number(entry.countEst) || 0;
        aggregateCounts(mgCounts[n], token, countEst);
      });
      const cms = partial[cmsKey];
      if (cms && typeof cms === 'object') {
        cmsByN[n].push(cms);
      }
    });
  });

  const ordered = {
    1: orderCandidates(Array.from(mgCounts[1].entries()), cmsByN[1], limitPerN[1]),
    2: orderCandidates(Array.from(mgCounts[2].entries()), cmsByN[2], limitPerN[2]),
    3: orderCandidates(Array.from(mgCounts[3].entries()), cmsByN[3], limitPerN[3]),
  };

  const unigramSet = new Set();
  const bigramSet = new Set();
  const trigramSet = new Set();
  const unigrams = [];
  const bigrams = [];
  const trigrams = [];

  ordered[1].forEach((token) => appendOrdered(unigramSet, unigrams, token));
  ordered[2].forEach((token) => appendOrdered(bigramSet, bigrams, token));
  ordered[3].forEach((token) => appendOrdered(trigramSet, trigrams, token));

  for (const token of bigrams) {
    const parts = splitTokens(token);
    parts.forEach((part) => appendOrdered(unigramSet, unigrams, part));
  }

  for (const token of trigrams) {
    const parts = splitTokens(token);
    parts.forEach((part) => appendOrdered(unigramSet, unigrams, part));
    if (parts.length >= 3) {
      appendOrdered(bigramSet, bigrams, `${parts[0]} ${parts[1]}`);
      appendOrdered(bigramSet, bigrams, `${parts[1]} ${parts[2]}`);
    }
  }

  const tokenInfo = new Map();
  const baseOrderCounters = { 1: 0, 2: 0, 3: 0 };
  const supportOrderCounters = { 1: 0, 2: 0, 3: 0 };

  const addToken = (token, n, isBase) => {
    const normalized = typeof token === 'string' ? token.trim() : '';
    if (!normalized) return;
    const existing = tokenInfo.get(normalized);
    if (existing) {
      if (isBase && !existing.isBase) {
        existing.isBase = true;
        existing.baseOrder = baseOrderCounters[n]++;
      }
      return;
    }
    if (tokenInfo.size >= MAX_UNION_CANDIDATES) {
      return;
    }
    tokenInfo.set(normalized, {
      n,
      isBase: !!isBase,
      baseOrder: isBase ? baseOrderCounters[n]++ : Number.POSITIVE_INFINITY,
      supportOrder: !isBase ? supportOrderCounters[n]++ : Number.POSITIVE_INFINITY,
    });
  };

  const addBundle = (items) => {
    if (!Array.isArray(items) || !items.length) return;
    const seen = new Set();
    const normalizedItems = [];
    for (const item of items) {
      if (!item) continue;
      const normalized = typeof item.token === 'string' ? item.token.trim() : '';
      if (!normalized || seen.has(normalized)) continue;
      seen.add(normalized);
      normalizedItems.push({ token: normalized, n: item.n, isBase: !!item.isBase });
    }
    if (!normalizedItems.length) return;
    let missing = 0;
    for (const item of normalizedItems) {
      if (!tokenInfo.has(item.token)) {
        missing += 1;
      }
    }
    if (tokenInfo.size + missing > MAX_UNION_CANDIDATES) {
      return;
    }
    normalizedItems.forEach((item) => {
      addToken(item.token, item.n, item.isBase);
    });
  };

  trigrams.forEach((token) => {
    if (tokenInfo.size >= MAX_UNION_CANDIDATES) return;
    const parts = splitTokens(token);
    const bundle = [];
    if (parts.length >= 3) {
      bundle.push({ token: parts[0], n: 1, isBase: false });
      bundle.push({ token: parts[1], n: 1, isBase: false });
      bundle.push({ token: parts[2], n: 1, isBase: false });
      bundle.push({ token: `${parts[0]} ${parts[1]}`, n: 2, isBase: false });
      bundle.push({ token: `${parts[1]} ${parts[2]}`, n: 2, isBase: false });
    }
    bundle.push({ token, n: 3, isBase: true });
    addBundle(bundle);
  });

  bigrams.forEach((token) => {
    if (tokenInfo.size >= MAX_UNION_CANDIDATES) return;
    const parts = splitTokens(token);
    const bundle = [];
    if (parts.length >= 2) {
      bundle.push({ token: parts[0], n: 1, isBase: false });
      bundle.push({ token: parts[1], n: 1, isBase: false });
    }
    bundle.push({ token, n: 2, isBase: true });
    addBundle(bundle);
  });

  unigrams.forEach((token) => {
    if (tokenInfo.size >= MAX_UNION_CANDIDATES) return;
    addBundle([{ token, n: 1, isBase: true }]);
  });

  const finalByN = { 1: [], 2: [], 3: [] };
  for (const [token, info] of tokenInfo.entries()) {
    if (!finalByN[info.n]) {
      finalByN[info.n] = [];
    }
    finalByN[info.n].push({ token, info });
  }

  const sortBuckets = (bucket) => bucket.sort((a, b) => {
    if (a.info.isBase !== b.info.isBase) {
      return a.info.isBase ? -1 : 1;
    }
    if (a.info.isBase) {
      return a.info.baseOrder - b.info.baseOrder;
    }
    return a.info.supportOrder - b.info.supportOrder;
  }).map((entry) => entry.token);

  finalByN[1] = sortBuckets(finalByN[1]);
  finalByN[2] = sortBuckets(finalByN[2]);
  finalByN[3] = sortBuckets(finalByN[3]);

  const combinedSet = new Set();
  finalByN[3].forEach((token) => combinedSet.add(token));
  finalByN[2].forEach((token) => combinedSet.add(token));
  finalByN[1].forEach((token) => combinedSet.add(token));

  return {
    candidates: Array.from(combinedSet),
    candidatesByN: finalByN,
    totalTokens,
  };
}

function* chunkString(text, chunkSize = RECOUNT_MAX_CHUNK_CHARS) {
  if (!text) return;
  const str = String(text);
  if (str.length <= chunkSize) {
    yield str;
    return;
  }
  for (let offset = 0; offset < str.length; offset += chunkSize) {
    yield str.slice(offset, offset + chunkSize);
  }
}

function* chunkMessages(messages, chunkSize = RECOUNT_MAX_CHUNK_CHARS) {
  if (!Array.isArray(messages) || !messages.length) return;
  let bucket = [];
  let bucketChars = 0;
  const flushBucket = () => {
    if (bucket.length) {
      const emitted = bucket;
      bucket = [];
      bucketChars = 0;
      return emitted;
    }
    return null;
  };
  for (const msg of messages) {
    const text = msg && typeof msg.text === 'string' ? msg.text : '';
    if (!text) continue;
    const entryChars = text.length;
    if (entryChars > chunkSize) {
      const emitted = flushBucket();
      if (emitted) {
        yield { messages: emitted };
      }
      for (const piece of chunkString(text, chunkSize)) {
        yield { text: piece };
      }
      continue;
    }
    if (bucket.length && bucketChars + entryChars > chunkSize) {
      const emitted = flushBucket();
      if (emitted) {
        yield { messages: emitted };
      }
    }
    bucket.push(msg);
    bucketChars += entryChars;
  }
  const emitted = flushBucket();
  if (emitted) {
    yield { messages: emitted };
  }
}

async function* iterateShardChunks(iterShardText, shardId) {
  const result = await iterShardText(shardId);
  if (!result) return;
  if (Array.isArray(result.messages)) {
    for (const chunk of chunkMessages(result.messages)) {
      if (!chunk) continue;
      if (Array.isArray(chunk.messages) && chunk.messages.length) {
        yield { messages: chunk.messages };
      } else if (typeof chunk.text === 'string' && chunk.text) {
        yield { text: chunk.text };
      }
    }
    return;
  }
  if (typeof result === 'string') {
    for (const chunk of chunkString(result)) {
      yield { text: chunk };
    }
    return;
  }
  if (result && typeof result.text === 'string') {
    for (const chunk of chunkString(result.text)) {
      yield { text: chunk };
    }
    return;
  }
  if (result && Array.isArray(result.text)) {
    for (const piece of result.text) {
      if (!piece) continue;
      for (const chunk of chunkString(piece)) {
        yield { text: chunk };
      }
    }
    return;
  }
  if (result && typeof result[Symbol.asyncIterator] === 'function') {
    for await (const piece of result) {
      if (!piece) continue;
      for (const chunk of chunkString(piece)) {
        yield { text: chunk };
      }
    }
  }
}

function pickPoolSize(totalShards) {
  if (!Number.isFinite(totalShards) || totalShards <= 0) {
    return 1;
  }
  let poolSize = Math.min(4, Math.max(1, Math.floor(totalShards)));
  const hc = typeof navigator !== 'undefined' && navigator ? navigator.hardwareConcurrency : null;
  if (typeof hc === 'number' && Number.isFinite(hc) && hc > 1) {
    const suggested = Math.max(1, Math.min(4, hc - 1));
    poolSize = Math.min(Math.max(poolSize, suggested), 4);
  }
  if (poolSize < 3 && totalShards >= 3) {
    poolSize = 3;
  }
  return poolSize;
}

function ensureEntry(map, token) {
  const normalized = typeof token === 'string' ? token.trim() : '';
  if (!normalized) {
    return null;
  }
  let entry = map.get(normalized);
  if (!entry) {
    const n = ngramLength(normalized);
    entry = {
      token: normalized,
      n,
      freq: 0,
    };
    if (n > 1) {
      entry.leftNeighbors = new Map();
      entry.rightNeighbors = new Map();
    }
    map.set(normalized, entry);
  }
  return entry;
}

function mergeNeighborMap(target, additions) {
  if (!(target instanceof Map)) {
    return;
  }
  if (!Array.isArray(additions)) {
    return;
  }
  for (const [neighbor, count] of additions) {
    if (!neighbor) continue;
    const next = (target.get(neighbor) || 0) + (Number(count) || 0);
    target.set(neighbor, next);
  }
}

function computeEntropy(neighborMap) {
  if (!(neighborMap instanceof Map) || neighborMap.size === 0) {
    return 0;
  }
  let total = 0;
  neighborMap.forEach((count) => {
    total += Number(count) || 0;
  });
  if (!total) return 0;
  let entropy = 0;
  neighborMap.forEach((count) => {
    const value = Number(count) || 0;
    if (!value) return;
    const p = value / total;
    entropy -= p * Math.log2(p);
  });
  return entropy;
}

function computePMI(freqXY, freqX, freqY, totalTokens) {
  const joint = Number(freqXY) || 0;
  const left = Number(freqX) || 0;
  const right = Number(freqY) || 0;
  if (!joint || !left || !right || !totalTokens) {
    return 0;
  }
  const numerator = joint * totalTokens;
  const denominator = left * right;
  if (!denominator) {
    return 0;
  }
  return Math.log2(numerator / denominator);
}

export async function exactRecount(candidates, iterShardText, onProgress, options = {}) {
  if (!Array.isArray(candidates) || !candidates.length) {
    return new Map();
  }
  if (typeof iterShardText !== 'function') {
    return new Map();
  }

  const shouldAbort = typeof options?.shouldAbort === 'function' ? options.shouldAbort : null;

  let shardIds = [];
  if (Array.isArray(iterShardText.shardIds)) {
    shardIds = iterShardText.shardIds;
  } else if (typeof iterShardText.getShardIds === 'function') {
    const maybeIds = await iterShardText.getShardIds();
    if (Array.isArray(maybeIds)) {
      shardIds = maybeIds;
    }
  }
  if (!shardIds.length) {
    return new Map();
  }

  const uniqueCandidates = Array.from(new Set(candidates));
  if (!uniqueCandidates.length) {
    return new Map();
  }

  const workerUrl = new URL('../workers/recount-worker.js', import.meta.url).href;
  const pool = new WorkerPool(pickPoolSize(shardIds.length), workerUrl);
  const globalStats = new Map();
  const candidateSet = new Set(uniqueCandidates);
  let aborted = false;

  try {
    let doneShards = 0;
    for (const shardId of shardIds) {
      if (shouldAbort?.()) {
        aborted = true;
        break;
      }
      const shardCounts = new Map();
      let shardTokensSeen = 0;

      for await (const chunk of iterateShardChunks(iterShardText, shardId)) {
        if (shouldAbort?.()) {
          aborted = true;
          break;
        }
        if (!chunk) continue;
        const payload = {
          shardId,
          candidates: uniqueCandidates,
          cutoff: iterShardText.cutoff,
          mask: iterShardText.mask,
        };
        if (typeof chunk.text === 'string') {
          payload.text = chunk.text;
        }
        if (Array.isArray(chunk.messages)) {
          payload.messages = chunk.messages;
        }
        const result = await pool.run(payload);
        if (result?.error) {
          throw new Error(result.error.message || 'Recount worker failed');
        }
        if (shouldAbort?.()) {
          aborted = true;
          break;
        }
        shardTokensSeen += Number(result?.tokensSeen) || 0;
        const countsByN = result?.counts || {};
        const neighborsByN = result?.neighbors || {};

        for (const key of Object.keys(countsByN)) {
          const entries = Array.isArray(countsByN[key]) ? countsByN[key] : [];
          for (const pair of entries) {
            if (!pair) continue;
            const [token, count] = pair;
            if (!candidateSet.has(token)) continue;
            const next = (shardCounts.get(token) || 0) + Number(count || 0);
            shardCounts.set(token, next);
          }
        }

        for (const key of Object.keys(neighborsByN)) {
          const bag = neighborsByN[key];
          if (!bag) continue;
          const leftEntries = Array.isArray(bag.left) ? bag.left : [];
          const rightEntries = Array.isArray(bag.right) ? bag.right : [];

          for (const [token, neighborList] of leftEntries) {
            if (!candidateSet.has(token)) continue;
            const entry = ensureEntry(globalStats, token);
            if (!entry || entry.n <= 1) continue;
            mergeNeighborMap(entry.leftNeighbors, neighborList);
          }

          for (const [token, neighborList] of rightEntries) {
            if (!candidateSet.has(token)) continue;
            const entry = ensureEntry(globalStats, token);
            if (!entry || entry.n <= 1) continue;
            mergeNeighborMap(entry.rightNeighbors, neighborList);
          }
        }
      }

      if (aborted) {
        break;
      }
      for (const [token, count] of shardCounts.entries()) {
        if (!count) continue;
        const entry = ensureEntry(globalStats, token);
        if (!entry) continue;
        entry.freq += Number(count) || 0;
      }

      doneShards += 1;
      if (typeof onProgress === 'function') {
        await onProgress({ doneShards, totalShards: shardIds.length, shardTokensSeen });
      }
    }
  } finally {
    await pool.terminate();
  }

  return globalStats;
}

export function scoreCandidates(statsMap, totalTokens, options = {}) {
  if (!(statsMap instanceof Map) || !statsMap.size) {
    return [];
  }
  const params = {
    ...DEFAULT_SCORING,
    ...(options || {}),
  };

  const entries = [];
  const unigramFreqs = new Map();
  const bigramFreqs = new Map();

  for (const [token, value] of statsMap.entries()) {
    if (!token || !value) continue;
    const freq = Number(value.freq ?? value.count ?? value) || 0;
    if (!freq) continue;
    const n = Number(value.n) || ngramLength(token);
    const leftNeighbors = value.leftNeighbors instanceof Map ? value.leftNeighbors : undefined;
    const rightNeighbors = value.rightNeighbors instanceof Map ? value.rightNeighbors : undefined;
    const entry = {
      token,
      n,
      freq,
      leftNeighbors,
      rightNeighbors,
      pmi: 0,
      score: 0,
      leftEntropy: 0,
      rightEntropy: 0,
      minEntropy: 0,
    };
    entries.push(entry);
    if (n === 1) {
      unigramFreqs.set(token, freq);
    } else if (n === 2) {
      bigramFreqs.set(token, freq);
    }
  }

  if (!entries.length) {
    return [];
  }

  const unigramPenalty = new Map();
  const bigramPenalty = new Map();

  for (const entry of entries) {
    if (entry.n === 1) {
      entry.score = entry.freq * params.delta;
      continue;
    }

    if (entry.freq < params.minFreq) {
      entry.score = 0;
      continue;
    }

    const parts = splitTokens(entry.token);
    const leftEntropy = computeEntropy(entry.leftNeighbors);
    const rightEntropy = computeEntropy(entry.rightNeighbors);
    entry.leftEntropy = leftEntropy;
    entry.rightEntropy = rightEntropy;
    const minEntropy = Math.min(leftEntropy, rightEntropy);
    entry.minEntropy = Number.isFinite(minEntropy) ? minEntropy : 0;

    if (entry.n === 2) {
      const [a, b] = parts;
      const freqA = unigramFreqs.get(a) || 0;
      const freqB = unigramFreqs.get(b) || 0;
      const pmi = computePMI(entry.freq, freqA, freqB, totalTokens);
      entry.pmi = pmi;
      if (pmi < params.minPMI) {
        entry.score = 0;
        continue;
      }
      entry.score = entry.freq * (1 + params.alpha * pmi) * (1 + params.beta * entry.minEntropy);
      if (isWhitelistedPhrase(parts)) {
        entry.score *= WHITELIST_BONUS;
      }
      const penaltyFactor = 0.85;
      unigramPenalty.set(a, (unigramPenalty.get(a) || 1) * penaltyFactor);
      unigramPenalty.set(b, (unigramPenalty.get(b) || 1) * penaltyFactor);
    } else if (entry.n === 3) {
      const [a, b, c] = parts;
      const freqA = unigramFreqs.get(a) || 0;
      const freqB = unigramFreqs.get(b) || 0;
      const freqC = unigramFreqs.get(c) || 0;
      const bigramAB = `${a} ${b}`;
      const bigramBC = `${b} ${c}`;
      const freqAB = bigramFreqs.get(bigramAB) || 0;
      const freqBC = bigramFreqs.get(bigramBC) || 0;
      const pmiAB = computePMI(freqAB, freqA, freqB, totalTokens);
      const pmiBC = computePMI(freqBC, freqB, freqC, totalTokens);
      let avgPMI = 0;
      const components = [];
      if (pmiAB > 0) components.push(pmiAB);
      if (pmiBC > 0) components.push(pmiBC);
      if (components.length) {
        avgPMI = components.reduce((sum, value) => sum + value, 0) / components.length;
      } else {
        const product = freqA * freqB * freqC;
        if (product > 0 && totalTokens > 0) {
          avgPMI = Math.log2((entry.freq * totalTokens) / product);
        }
      }
      entry.pmi = avgPMI;
      if (avgPMI < params.minPMI) {
        entry.score = 0;
        continue;
      }
      entry.score = entry.freq
        * (1 + params.alpha * avgPMI)
        * (1 + params.beta * entry.minEntropy)
        * params.gamma;

      if (isWhitelistedPhrase(parts)) {
        entry.score *= WHITELIST_BONUS;
      }

      const unigramFactor = 0.75;
      unigramPenalty.set(a, (unigramPenalty.get(a) || 1) * unigramFactor);
      unigramPenalty.set(b, (unigramPenalty.get(b) || 1) * unigramFactor);
      unigramPenalty.set(c, (unigramPenalty.get(c) || 1) * unigramFactor);
      const bigramFactor = 0.85;
      bigramPenalty.set(bigramAB, (bigramPenalty.get(bigramAB) || 1) * bigramFactor);
      bigramPenalty.set(bigramBC, (bigramPenalty.get(bigramBC) || 1) * bigramFactor);
    }
  }

  for (const entry of entries) {
    if (entry.n === 1) {
      const penalty = unigramPenalty.get(entry.token);
      if (penalty != null && penalty < 1) {
        entry.score *= penalty;
      }
      continue;
    }
    if (entry.n === 2) {
      const penalty = bigramPenalty.get(entry.token);
      if (penalty != null && penalty < 1) {
        entry.score *= penalty;
      }
    }
  }

  const usable = entries
    .filter((entry) => entry.freq > 0 && (entry.score > 0 || entry.n === 1))
    .sort((a, b) => {
      if (b.score !== a.score) {
        return b.score - a.score;
      }
      if (b.freq !== a.freq) {
        return b.freq - a.freq;
      }
      if (a.n !== b.n) {
        return a.n - b.n;
      }
      return a.token.localeCompare(b.token);
    });

  if (!usable.length) {
    return [];
  }

  return usable;
}

function normalizeTopEntry(entry) {
  if (!entry) return null;
  const token = typeof entry.token === 'string' ? entry.token.trim() : '';
  if (!token) return null;
  const freq = Number(entry.freq) || 0;
  const score = Number(entry.score) || 0;
  const n = Number.isFinite(entry.n) && entry.n > 0 ? Number(entry.n) : ngramLength(token);
  if (!(score > 0 || n === 1)) {
    return null;
  }
  return {
    token,
    score,
    freq,
    n,
  };
}

function cloneViewList(list) {
  if (!Array.isArray(list)) return [];
  return list.map((entry) => ({
    token: entry && typeof entry.token === 'string' ? entry.token : '',
    score: Number(entry?.score) || 0,
    freq: Number(entry?.freq) || 0,
    n: Number.isFinite(entry?.n) && entry?.n > 0 ? Number(entry.n) : ngramLength(entry?.token || ''),
  })).filter((entry) => entry.token);
}

export function buildTopViews(sortedCandidates, limit = 10) {
  const sorted = Array.isArray(sortedCandidates) ? sortedCandidates : [];
  const maxItems = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : 10;
  if (!sorted.length || maxItems <= 0) {
    return { phrases: [], words: [] };
  }

  const phrases = [];
  const words = [];

  for (const raw of sorted) {
    const normalized = normalizeTopEntry(raw);
    if (!normalized) continue;
    if (normalized.n > 1 && phrases.length < maxItems) {
      phrases.push(normalized);
    }
    if (normalized.n === 1 && words.length < maxItems) {
      words.push(normalized);
    }
    if (phrases.length >= maxItems && words.length >= maxItems) {
      break;
    }
  }

  const phraseView = phrases.slice(0, maxItems);
  if (phraseView.length < maxItems) {
    for (let i = 0; i < words.length && phraseView.length < maxItems; i += 1) {
      phraseView.push(words[i]);
    }
  }
  const wordView = words.slice(0, maxItems);

  return {
    phrases: cloneViewList(phraseView),
    words: cloneViewList(wordView),
  };
}

function sameTokenMultiset(prevTokens, nextTokens) {
  if (!Array.isArray(prevTokens) || !Array.isArray(nextTokens)) {
    return false;
  }
  if (prevTokens.length !== nextTokens.length) {
    return false;
  }
  const counts = new Map();
  for (const token of prevTokens) {
    counts.set(token, (counts.get(token) || 0) + 1);
  }
  for (const token of nextTokens) {
    if (!counts.has(token)) {
      return false;
    }
    const next = counts.get(token) - 1;
    if (next === 0) {
      counts.delete(token);
    } else {
      counts.set(token, next);
    }
  }
  return counts.size === 0;
}

function withinSwapTolerance(prevTokens, nextTokens, maxSwaps = 1) {
  if (!Array.isArray(prevTokens) || !Array.isArray(nextTokens)) {
    return false;
  }
  if (prevTokens.length !== nextTokens.length) {
    return false;
  }
  const tolerance = Math.max(0, Math.floor(Number.isFinite(maxSwaps) ? maxSwaps : 1));
  const mismatches = [];
  for (let i = 0; i < prevTokens.length; i += 1) {
    if (prevTokens[i] !== nextTokens[i]) {
      mismatches.push(i);
      if (tolerance === 0) {
        return false;
      }
      if (mismatches.length > 2 * tolerance) {
        return false;
      }
    }
  }
  if (mismatches.length === 0) {
    return true;
  }
  if (tolerance === 0) {
    return false;
  }
  if (mismatches.length !== 2) {
    return false;
  }
  const [i, j] = mismatches;
  if (j !== i + 1) {
    return false;
  }
  return prevTokens[i] === nextTokens[j] && prevTokens[j] === nextTokens[i];
}

function reconcileList(prevList, nextList, maxSwaps) {
  const next = Array.isArray(nextList) ? nextList : [];
  if (!next.length) {
    return [];
  }
  if (!Array.isArray(prevList) || !prevList.length) {
    return cloneViewList(next);
  }
  const prevTokens = prevList.map((entry) => entry.token);
  const nextTokens = next.map((entry) => entry.token);
  if (!sameTokenMultiset(prevTokens, nextTokens)) {
    return cloneViewList(next);
  }
  if (withinSwapTolerance(prevTokens, nextTokens, maxSwaps)) {
    return cloneViewList(next);
  }
  const prevByToken = new Map();
  const nextByToken = new Map();
  prevList.forEach((entry) => {
    if (entry && entry.token) {
      prevByToken.set(entry.token, entry);
    }
  });
  next.forEach((entry) => {
    if (entry && entry.token) {
      nextByToken.set(entry.token, entry);
    }
  });
  const EPSILON = 1e-6;
  let metricsIdentical = true;
  for (const [token, nextEntry] of nextByToken.entries()) {
    const prevEntry = prevByToken.get(token);
    if (!prevEntry) {
      metricsIdentical = false;
      break;
    }
    const scoreDiff = Math.abs((Number(prevEntry.score) || 0) - (Number(nextEntry.score) || 0));
    const freqDiff = Math.abs((Number(prevEntry.freq) || 0) - (Number(nextEntry.freq) || 0));
    if (scoreDiff > EPSILON || freqDiff > 0) {
      metricsIdentical = false;
      break;
    }
  }
  if (metricsIdentical) {
    return cloneViewList(prevList);
  }
  return cloneViewList(next);
}

export function reconcileTopViews(previousViews, nextViews, options = {}) {
  const prev = previousViews && typeof previousViews === 'object' ? previousViews : { phrases: [], words: [] };
  const next = nextViews && typeof nextViews === 'object' ? nextViews : { phrases: [], words: [] };
  const maxSwaps = Number.isFinite(options?.maxSwaps) ? options.maxSwaps : 1;
  return {
    phrases: reconcileList(prev.phrases, next.phrases, maxSwaps),
    words: reconcileList(prev.words, next.words, maxSwaps),
  };
}
