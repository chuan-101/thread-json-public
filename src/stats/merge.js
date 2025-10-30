const CMS_SEEDS = [0x1b873593, 0xcc9e2d51, 0x9e3779b1, 0x85ebca6b];

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

export async function exactRecount(candidates, iterShardTokens) {
  if (!Array.isArray(candidates) || !candidates.length) {
    return new Map();
  }
  if (typeof iterShardTokens !== 'function') {
    return new Map();
  }

  const candidateSet = new Set(candidates);
  const counts = new Map();

  let shardIds = [];
  if (Array.isArray(iterShardTokens.shardIds)) {
    shardIds = iterShardTokens.shardIds;
  } else if (typeof iterShardTokens.getShardIds === 'function') {
    const maybeIds = await iterShardTokens.getShardIds();
    if (Array.isArray(maybeIds)) {
      shardIds = maybeIds;
    }
  }

  for (const shardId of shardIds) {
    const iterator = await iterShardTokens(shardId);
    if (!iterator || typeof iterator[Symbol.asyncIterator] !== 'function') {
      continue;
    }
    for await (const token of iterator) {
      if (!candidateSet.has(token)) continue;
      const nextCount = (counts.get(token) || 0) + 1;
      counts.set(token, nextCount);
    }
  }

  return counts;
}
