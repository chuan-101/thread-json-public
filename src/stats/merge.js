import { WorkerPool } from '../workers/pool.js';

const CMS_SEEDS = [0x1b873593, 0xcc9e2d51, 0x9e3779b1, 0x85ebca6b];
const RECOUNT_MAX_CHUNK_CHARS = 512_000; // ~1MB of UTF-16 text

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

export function mergePartials(partials, K = 3000) {
  if (!Array.isArray(partials) || !partials.length) {
    return { candidates: [], totalTokens: 0 };
  }

  const candidateSet = new Set();
  const mgCounts = new Map();
  let totalTokens = 0;

  partials.forEach((partial) => {
    if (!partial) return;
    const { mgTopK, totalTokens: shardTokens } = partial;
    if (Array.isArray(mgTopK)) {
      mgTopK.forEach((entry) => {
        const token = entry && typeof entry.token === 'string' ? entry.token : null;
        if (token) {
          candidateSet.add(token);
          const countEst = Number(entry.countEst) || 0;
          if (countEst > 0) {
            mgCounts.set(token, (mgCounts.get(token) || 0) + countEst);
          }
        }
      });
    }
    if (Number.isFinite(shardTokens)) {
      totalTokens += shardTokens;
    }
  });

  const allCandidates = Array.from(candidateSet);
  if (allCandidates.length <= K) {
    return { candidates: allCandidates, totalTokens };
  }

  const scored = allCandidates.map((token) => {
    let upperBound = 0;
    for (const partial of partials) {
      if (!partial || !partial.cms) continue;
      upperBound += cmsEstimate(token, partial.cms);
    }
    return { token, upperBound, mgCount: mgCounts.get(token) || 0 };
  });

  scored.sort((a, b) => {
    if (b.upperBound !== a.upperBound) {
      return b.upperBound - a.upperBound;
    }
    if (b.mgCount !== a.mgCount) {
      return b.mgCount - a.mgCount;
    }
    return a.token.localeCompare(b.token);
  });

  const topK = scored.slice(0, K).map((item) => item.token);
  return { candidates: topK, totalTokens };
}

function toStopwordArray(stopwords) {
  if (!stopwords) return [];
  if (Array.isArray(stopwords)) return stopwords;
  if (stopwords instanceof Set) return Array.from(stopwords);
  if (typeof stopwords === 'string') {
    return stopwords.split(/[\s,]+/u).filter(Boolean);
  }
  return [];
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

async function* iterateShardChunks(iterShardText, shardId) {
  const result = await iterShardText(shardId);
  if (typeof result === 'string') {
    yield* chunkString(result);
    return;
  }
  if (result && typeof result[Symbol.asyncIterator] === 'function') {
    for await (const piece of result) {
      if (!piece) continue;
      yield* chunkString(piece);
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

export async function exactRecount(candidates, iterShardText, onProgress) {
  if (!Array.isArray(candidates) || !candidates.length) {
    return new Map();
  }
  if (typeof iterShardText !== 'function') {
    return new Map();
  }

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

  const stopwordPayload = toStopwordArray(iterShardText.stopwords);
  const workerUrl = new URL('../workers/recount-worker.js', import.meta.url).href;
  const pool = new WorkerPool(pickPoolSize(shardIds.length), workerUrl);
  const globalCounts = new Map();
  const candidateSet = new Set(uniqueCandidates);

  try {
    let doneShards = 0;
    for (const shardId of shardIds) {
      const shardCounts = new Map();
      let shardTokensSeen = 0;

      for await (const chunk of iterateShardChunks(iterShardText, shardId)) {
        if (!chunk) continue;
        const result = await pool.run({
          shardId,
          text: chunk,
          candidates: uniqueCandidates,
          stopwords: stopwordPayload,
        });
        if (result?.error) {
          throw new Error(result.error.message || 'Recount worker failed');
        }
        shardTokensSeen += Number(result?.tokensSeen) || 0;
        const pairs = Array.isArray(result?.counts) ? result.counts : [];
        for (const entry of pairs) {
          if (!entry) continue;
          const [token, count] = entry;
          if (!candidateSet.has(token)) continue;
          const next = (shardCounts.get(token) || 0) + Number(count || 0);
          shardCounts.set(token, next);
        }
      }

      for (const [token, count] of shardCounts.entries()) {
        if (!count) continue;
        globalCounts.set(token, (globalCounts.get(token) || 0) + count);
      }

      doneShards += 1;
      onProgress?.({ doneShards, totalShards: shardIds.length, shardTokensSeen });
    }
  } finally {
    await pool.terminate();
  }

  return globalCounts;
}
