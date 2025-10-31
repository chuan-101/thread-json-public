import { tokenizeIter, inferTokenScript } from '../stats/tokenize.js';
import { applyMask } from '../stats/mask.js';

const BATCH_YIELD_INTERVAL = 8192;
const START_TOKEN = '__START__';
const END_TOKEN = '__END__';

function splitTokens(phrase) {
  if (typeof phrase !== 'string') return [];
  return phrase
    .split(/\s+/)
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
}

function bump(map, key, delta = 1) {
  const next = (map.get(key) || 0) + delta;
  map.set(key, next);
}

function bumpNeighbor(store, key, neighbor) {
  if (!store.has(key)) {
    store.set(key, new Map());
  }
  const inner = store.get(key);
  bump(inner, neighbor, 1);
}

function mapToEntries(map) {
  return Array.from(map.entries()).map(([token, inner]) => [
    token,
    Array.from(inner.entries()),
  ]);
}

self.onmessage = async (event) => {
  const { shardId, text, messages, candidates, cutoff, mask } = event.data || {};
  try {
    const candidateArray = Array.isArray(candidates) ? candidates : [];
    const candidateSets = {
      1: new Set(),
      2: new Set(),
      3: new Set(),
    };
    for (const cand of candidateArray) {
      const parts = splitTokens(cand);
      const n = parts.length;
      if (n >= 1 && n <= 3) {
        candidateSets[n].add(parts.join(' '));
      }
    }

    const counts = {
      1: new Map(),
      2: new Map(),
      3: new Map(),
    };
    const leftNeighbors = {
      2: new Map(),
      3: new Map(),
    };
    const rightNeighbors = {
      2: new Map(),
      3: new Map(),
    };
    let tokensSeen = 0;
    let lastYieldCount = 0;

    const effectiveCutoff = typeof cutoff === 'number' && Number.isFinite(cutoff) ? cutoff : null;

    const processText = async (input) => {
      const masked = applyMask(typeof input === 'string' ? input : '', mask);
      const tokens = Array.from(tokenizeIter(masked));
      if (!tokens.length) {
        return;
      }

      const annotated = tokens.map((token) => ({
        token,
        script: inferTokenScript(token),
      }));

      tokensSeen += annotated.length;

      for (let i = 0; i < annotated.length; i += 1) {
        const current = annotated[i];
        const unigram = current.token;
        if (candidateSets[1].has(unigram)) {
          bump(counts[1], unigram, 1);
        }

        if (i >= 1) {
          const prev = annotated[i - 1];
          if (prev.script === current.script) {
            const bigram = `${prev.token} ${current.token}`;
            if (candidateSets[2].has(bigram)) {
              bump(counts[2], bigram, 1);
              const leftIdx = i - 2;
              const rightIdx = i + 1;
              const leftNeighbor = leftIdx >= 0 ? annotated[leftIdx].token : START_TOKEN;
              const rightNeighbor = rightIdx < annotated.length ? annotated[rightIdx].token : END_TOKEN;
              bumpNeighbor(leftNeighbors[2], bigram, leftNeighbor);
              bumpNeighbor(rightNeighbors[2], bigram, rightNeighbor);
            }
          }
        }

        if (i >= 2) {
          const prev1 = annotated[i - 1];
          const prev2 = annotated[i - 2];
          if (prev1.script === current.script && prev2.script === current.script) {
            const trigram = `${prev2.token} ${prev1.token} ${current.token}`;
            if (candidateSets[3].has(trigram)) {
              bump(counts[3], trigram, 1);
              const leftIdx = i - 3;
              const rightIdx = i + 1;
              const leftNeighbor = leftIdx >= 0 ? annotated[leftIdx].token : START_TOKEN;
              const rightNeighbor = rightIdx < annotated.length ? annotated[rightIdx].token : END_TOKEN;
              bumpNeighbor(leftNeighbors[3], trigram, leftNeighbor);
              bumpNeighbor(rightNeighbors[3], trigram, rightNeighbor);
            }
          }
        }
      }

      if (tokensSeen - lastYieldCount >= BATCH_YIELD_INTERVAL) {
        await new Promise((resolve) => setTimeout(resolve, 0));
        lastYieldCount = tokensSeen;
      }
    };

    if (Array.isArray(messages)) {
      for (const msg of messages) {
        if (!msg || typeof msg.text !== 'string' || !msg.text) continue;
        const ts = typeof msg.ts === 'number' && Number.isFinite(msg.ts) ? msg.ts : null;
        if (effectiveCutoff != null && (ts == null || ts < effectiveCutoff)) {
          continue;
        }
        await processText(msg.text);
      }
    } else {
      await processText(typeof text === 'string' ? text : '');
    }

    const countsPayload = {};
    for (const n of [1, 2, 3]) {
      if (counts[n].size) {
        countsPayload[n] = Array.from(counts[n].entries());
      }
    }

    const neighborsPayload = {};
    for (const n of [2, 3]) {
      const leftMap = leftNeighbors[n];
      const rightMap = rightNeighbors[n];
      if (leftMap.size || rightMap.size) {
        neighborsPayload[n] = {};
        if (leftMap.size) {
          neighborsPayload[n].left = mapToEntries(leftMap);
        }
        if (rightMap.size) {
          neighborsPayload[n].right = mapToEntries(rightMap);
        }
      }
    }

    self.postMessage({
      shardId,
      counts: countsPayload,
      neighbors: neighborsPayload,
      tokensSeen,
    });
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    self.postMessage({ shardId, error: { message: error.message } });
    throw error;
  }
};
