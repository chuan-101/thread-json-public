import { tokenizeIter } from '../stats/tokenize.js';
import { applyMask } from '../stats/mask.js';

const BATCH_YIELD_INTERVAL = 8192;

self.onmessage = async (event) => {
  const { shardId, text, messages, candidates, cutoff, mask } = event.data || {};
  try {
    const candidateArray = Array.isArray(candidates) ? candidates : [];
    const candidateSet = new Set(candidateArray);
    const counts = new Map();
    let tokensSeen = 0;
    let lastYieldCount = 0;

    const effectiveCutoff = typeof cutoff === 'number' && Number.isFinite(cutoff) ? cutoff : null;

    const processText = async (input) => {
      const masked = applyMask(typeof input === 'string' ? input : '', mask);
      const iterable = tokenizeIter(masked);
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
