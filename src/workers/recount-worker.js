import { tokenizeIter, createStopwordSet } from '../stats/tokenize.js';

const BATCH_YIELD_INTERVAL = 8192;

self.onmessage = async (event) => {
  const { shardId, text, candidates, stopwords } = event.data || {};
  try {
    const candidateArray = Array.isArray(candidates) ? candidates : [];
    const candidateSet = new Set(candidateArray);
    const stopwordSet = createStopwordSet(stopwords);
    const counts = new Map();
    let tokensSeen = 0;
    let lastYieldCount = 0;

    const iterable = tokenizeIter(typeof text === 'string' ? text : '', stopwordSet);
    for (const token of iterable) {
      tokensSeen += 1;
      if (candidateSet.has(token)) {
        counts.set(token, (counts.get(token) || 0) + 1);
      }
      if (tokensSeen - lastYieldCount >= BATCH_YIELD_INTERVAL) {
        await new Promise((resolve) => setTimeout(resolve, 0));
        lastYieldCount = tokensSeen;
      }
    }

    self.postMessage({
      shardId,
      counts: Array.from(counts.entries()),
      tokensSeen,
    });
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    self.postMessage({ shardId, error: { message: error.message } });
    throw error;
  }
};
