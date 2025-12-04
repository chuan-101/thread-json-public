import test from 'node:test';
import assert from 'node:assert/strict';

import {
  cutoff90Days,
  cutoff365Days,
  filterMessagesByWindow,
  computeModelShare,
} from '../src/stats/models.js';
import {
  resetYearAgg,
  bumpYearAgg,
  finalizeYearAgg,
  computeYearlyMetrics,
} from '../src/stats/timeline.js';
import { mergePartials, scoreCandidates, buildTopViews, reconcileTopViews } from '../src/stats/merge.js';
import { tokenize, WHITELIST } from '../src/stats/tokenize.js';

const DAY_MS = 24 * 3600 * 1000;

test('rolling cutoffs move one day when clock shifts', () => {
  const baseNow = Date.UTC(2024, 5, 1);
  const cutoff90 = cutoff90Days(baseNow);
  const forwardCutoff = cutoff90Days(baseNow + DAY_MS);
  const backwardCutoff = cutoff90Days(baseNow - DAY_MS);

  assert.equal(forwardCutoff - cutoff90, DAY_MS, 'forward shift should advance cutoff by one day');
  assert.equal(cutoff90 - backwardCutoff, DAY_MS, 'backward shift should rewind cutoff by one day');

  const nearCutoffMsg = { ts: cutoff90 - 1000 };
  assert.equal(filterMessagesByWindow([nearCutoffMsg], cutoff90).length, 0, 'message just before cutoff excluded');
  assert.equal(
    filterMessagesByWindow([nearCutoffMsg], backwardCutoff).length,
    1,
    'rolling backward should include recent-enough message',
  );
  const yearlyCutoff = cutoff365Days(baseNow);
  assert.ok(yearlyCutoff < cutoff90, '365-day cutoff extends beyond the 90-day window');
  assert.equal(
    filterMessagesByWindow([nearCutoffMsg], forwardCutoff).length,
    0,
    'rolling forward tightens window and still excludes message',
  );
});

test('computeModelShare filters to assistant messages within the 12-month window', () => {
  const baseNow = Date.UTC(2024, 6, 1);
  const cutoff = cutoff365Days(baseNow);
  const withinWindowTs = cutoff + 10_000;
  const olderTs = cutoff - 10_000;

  const messages = [
    { ts: withinWindowTs, role: 'assistant', model: 'gpt-4o', text: 'hi' },
    { ts: withinWindowTs, role: 'user', model: 'gpt-4o', text: 'ignored' },
    { ts: withinWindowTs, role: 'assistant', model: 'gpt-3.5', text: 'hello there' },
    { ts: olderTs, role: 'assistant', model: 'gpt-4o', text: 'old' },
  ];

  const { total, entries, buckets } = computeModelShare(messages, { now: baseNow, cutoff });
  assert.equal(total, 2, 'only assistant messages in the window should contribute');
  assert.deepEqual(
    entries.map((e) => e.model),
    ['gpt-3.5', 'gpt-4o'],
    'model entries include assistant-only models ordered deterministically',
  );
  assert.ok(Array.isArray(buckets) && buckets.length >= 12, 'buckets cover at least the last year');
  const bucketWithTotals = buckets.find((b) => b.total > 0);
  assert.ok(bucketWithTotals, 'buckets contain at least one month with totals');
  assert.ok(bucketWithTotals.models.some((m) => m.model === 'gpt-4o'), 'monthly bucket tracks model totals');
});

test('computeModelShare ignores models older than 12 months and balances totals', () => {
  const baseNow = Date.UTC(2024, 11, 15);
  const cutoff = cutoff365Days(baseNow);
  const within30Days = baseNow - 30 * DAY_MS;
  const within200Days = baseNow - 200 * DAY_MS;
  const olderThanYear = baseNow - 400 * DAY_MS;

  const messages = [
    { ts: within30Days, role: 'assistant', model: 'gpt-4o', text: 'recent reply' },
    { ts: within200Days, role: 'assistant', model: 'gpt-4.1', text: 'mid-year answer' },
    { ts: olderThanYear, role: 'assistant', model: 'gpt-4o', text: 'too old' },
  ];

  const { total, entries, buckets } = computeModelShare(messages, { now: baseNow, cutoff });
  assert.equal(total, 2, 'only assistant messages within the last 12 months should count toward totals');
  const shares = Object.fromEntries(entries.map(({ model, share }) => [model, share]));
  assert.ok(Math.abs(shares['gpt-4.1'] - 0.5) < 1e-6, 'mid-year model carries half the share');
  assert.ok(Math.abs(shares['gpt-4o'] - 0.5) < 1e-6, 'recent model carries half the share');

  const bucketTotals = buckets.reduce((sum, bucket) => sum + bucket.total, 0);
  assert.equal(bucketTotals, total, 'bucket totals should match the overall total within the window');

  const monthKey = `${new Date(within200Days).getUTCFullYear()}-${String(
    new Date(within200Days).getUTCMonth() + 1,
  ).padStart(2, '0')}`;
  const midBucket = buckets.find((bucket) => bucket.key === monthKey);
  assert.ok(midBucket && midBucket.total > 0, 'mid-year bucket should accumulate model counts');
  assert.ok(
    buckets.filter((bucket) => bucket.end < cutoff).every((bucket) => bucket.total === 0),
    'no bucket before the 12-month cutoff should accumulate totals',
  );
});

test('yearly overview aggregates message counts and activity metrics', () => {
  resetYearAgg();
  const jan1 = Date.UTC(2023, 0, 1);
  const jan2 = Date.UTC(2023, 0, 2);
  const jan4 = Date.UTC(2023, 0, 4);

  bumpYearAgg(jan1, 'assistant', 2, 2);
  bumpYearAgg(jan2, 'user', 3, 0);
  bumpYearAgg(jan4, 'assistant', 3, 1);

  const summary = finalizeYearAgg();
  assert.ok(summary[2023], 'year entry should exist');
  const stats2023 = summary[2023];

  assert.equal(stats2023.totalMessages, 3, 'message count should include all roles');
  assert.equal(stats2023.totalChars, 8, 'character count includes all roles with newline normalization');
  assert.equal(stats2023.assistantMsgs, 2, 'assistant message count increments per assistant role');
  assert.equal(stats2023.assistantChars, 5, 'assistant character totals only include assistant role');
  assert.ok(Array.isArray(stats2023.charsByMonth), 'monthly breakdown should be present');
  assert.equal(stats2023.charsByMonth[0], 8, 'January should accumulate all characters in the sample');
  assert.equal(stats2023.images, 3, 'image totals accumulate per message');

  const activeSet = stats2023.activeDays;
  const activeDayCount = activeSet instanceof Set ? activeSet.size : Array.isArray(activeSet) ? activeSet.length : 0;
  assert.equal(activeDayCount, 3, 'active days track distinct calendar days');

  resetYearAgg();
  assert.deepEqual(finalizeYearAgg(), {}, 'reset clears accumulator state');
});

test('computeYearlyMetrics derives display-friendly yearly stats', () => {
  resetYearAgg();
  const jan1 = Date.UTC(2024, 0, 1);
  const feb2 = Date.UTC(2024, 1, 2);
  const mar3 = Date.UTC(2024, 2, 3);

  bumpYearAgg(jan1, 'assistant', 120, 1);
  bumpYearAgg(feb2, 'user', 90, 0);
  bumpYearAgg(mar3, 'assistant', 60, 0);

  const summary = finalizeYearAgg();
  const metrics = computeYearlyMetrics(summary);
  const stats2024 = metrics[2024];

  assert.ok(stats2024, 'derived metrics include the year entry');
  assert.equal(stats2024.messages, 3, 'total messages surface directly');
  assert.equal(
    stats2024.avgCharsPerActiveDay,
    90,
    'average characters per active day rounds to a single decimal place',
  );
  assert.equal(stats2024.mostActiveMonth, 1, 'January carries the most characters');
  assert.equal(
    stats2024.avgAssistantMsgLen,
    90,
    'assistant message averages divide assistant characters by assistant messages',
  );
  assert.ok(stats2024.activeDays instanceof Set, 'active days remain a Set copy');
  assert.notStrictEqual(
    stats2024.activeDays,
    summary[2024].activeDays,
    'active day Set is copied for isolation',
  );
  assert.deepEqual(
    stats2024.charsByMonth,
    summary[2024].charsByMonth,
    'monthly arrays are copied while preserving values',
  );
});

test('mergePartials unions candidates by n-gram order and preserves determinism', () => {
  const cmsTable = new Uint32Array(16).fill(32);
  const cms = { depth: 2, width: 8, table: cmsTable };
  const partials = [
    {
      shardId: 1,
      totalTokens: 100,
      uTopK: [
        { token: 'hello', countEst: 50 },
        { token: 'world', countEst: 35 },
      ],
      bTopK: [
        { token: 'hello world', countEst: 28 },
        { token: 'world peace', countEst: 18 },
      ],
      tTopK: [
        { token: 'hello brave world', countEst: 12 },
      ],
      cmsU: cms,
      cmsB: cms,
      cmsT: cms,
    },
    {
      shardId: 2,
      totalTokens: 80,
      uTopK: [
        { token: 'peace', countEst: 20 },
        { token: 'brave', countEst: 16 },
      ],
      bTopK: [
        { token: 'brave world', countEst: 14 },
      ],
      tTopK: [
        { token: 'make the world', countEst: 9 },
      ],
      cmsU: cms,
      cmsB: cms,
      cmsT: cms,
    },
  ];

  const first = mergePartials(partials, { limitPerN: { 1: 3, 2: 3, 3: 2 } });
  const second = mergePartials(partials, { limitPerN: { 1: 3, 2: 3, 3: 2 } });

  assert.deepEqual(first, second, 'repeated runs should be deterministic');
  assert.equal(first.totalTokens, 180, 'total tokens sum across shards');
  assert.deepEqual(
    first.candidatesByN[3],
    ['hello brave world', 'make the world'],
    'trigram union retains ordering up to configured limit',
  );
  assert.deepEqual(
    first.candidatesByN[2].slice(0, 3),
    ['hello world', 'world peace', 'brave world'],
    'bigram ordering honors top-k before support expansions',
  );
  assert.ok(
    first.candidatesByN[2].includes('make the')
      && first.candidatesByN[2].includes('the world'),
    'trigram support bigrams are appended for PMI calculations',
  );
  assert.ok(
    ['hello', 'world', 'peace', 'brave', 'make', 'the'].every((token) => first.candidatesByN[1].includes(token)),
    'unigram union includes phrase components for downstream scoring',
  );
  assert.ok(
    first.candidates.includes('hello brave world')
      && first.candidates.includes('make the world'),
    'flattened candidate list includes merged n-grams',
  );
});

test('tokenize normalizes aliases before downstream scoring', () => {
  const tokens = tokenize('Chat_GPT + Open_AI & GPT_4');
  assert.deepEqual(tokens, ['chatgpt', 'openai', 'gpt4'], 'underscored variants collapse to canonical tokens');
});

test('whitelisted phrases receive a modest scoring bonus', () => {
  assert.ok(
    WHITELIST.has('openai') && WHITELIST.has('chatgpt'),
    'expected canonical tokens should be whitelisted',
  );

  const stats = new Map();
  stats.set('openai', { n: 1, freq: 80 });
  stats.set('chatgpt', { n: 1, freq: 70 });
  stats.set('hello', { n: 1, freq: 80 });
  stats.set('world', { n: 1, freq: 70 });

  const leftContext = [['__START__', 35]];
  const rightContext = [['rocks', 30]];

  stats.set('openai chatgpt', {
    n: 2,
    freq: 45,
    leftNeighbors: new Map(leftContext),
    rightNeighbors: new Map(rightContext),
  });
  stats.set('hello world', {
    n: 2,
    freq: 45,
    leftNeighbors: new Map(leftContext),
    rightNeighbors: new Map(rightContext),
  });

  const scored = scoreCandidates(stats, 500);
  const openaiEntry = scored.find((entry) => entry.token === 'openai chatgpt');
  const helloEntry = scored.find((entry) => entry.token === 'hello world');

  assert.ok(openaiEntry && helloEntry, 'both candidate phrases should be scored');
  assert.ok(openaiEntry.score > helloEntry.score, 'whitelisted phrase should outrank the baseline');
  const ratio = openaiEntry.score / helloEntry.score;
  assert.ok(ratio > 1.05 && ratio < 1.2, 'bonus stays within the intended modest range');
});

test('scoreCandidates favors multi-token phrases over single words', () => {
  const stats = new Map();
  stats.set('hello', { n: 1, freq: 100 });
  stats.set('world', { n: 1, freq: 90 });
  stats.set('now', { n: 1, freq: 60 });
  stats.set('greetings', { n: 1, freq: 20 });

  stats.set(
    'hello world',
    {
      n: 2,
      freq: 65,
      leftNeighbors: new Map([
        ['__START__', 60],
      ]),
      rightNeighbors: new Map([
        ['now', 45],
        ['again', 10],
      ]),
    },
  );
  stats.set(
    'world now',
    {
      n: 2,
      freq: 45,
      leftNeighbors: new Map([
        ['world', 10],
        ['hello', 25],
      ]),
      rightNeighbors: new Map([
        ['!', 20],
        ['__END__', 12],
      ]),
    },
  );

  stats.set(
    'hello world now',
    {
      n: 3,
      freq: 50,
      leftNeighbors: new Map([
        ['__START__', 20],
        ['greetings', 10],
      ]),
      rightNeighbors: new Map([
        ['!', 15],
        ['__END__', 10],
      ]),
    },
  );

  const scored = scoreCandidates(stats, 400);
  assert.ok(scored.length > 0, 'scoring produces ranked candidates');
  const topTokens = scored.slice(0, 3).map((entry) => entry.token);
  assert.equal(topTokens[0], 'hello world now', 'trigram outranks others due to PMI bonus');
  assert.equal(topTokens[1], 'hello world', 'strong bigram is prioritized after trigram');
  assert.ok(
    scored.find((entry) => entry.token === 'hello')?.score
      < scored.find((entry) => entry.token === 'hello world')?.score,
    'unigram score remains below phrase counterpart after down-weighting',
  );
});

test('buildTopViews prioritizes phrases and deterministically backfills unigrams', () => {
  const sorted = [
    { token: 'alpha beta gamma', n: 3, score: 14.5, freq: 60 },
    { token: 'alpha beta', n: 2, score: 12, freq: 75 },
    { token: 'delta epsilon', n: 2, score: 9, freq: 50 },
    { token: 'theta iota', n: 2, score: 8, freq: 42 },
    { token: 'alpha', n: 1, score: 3.5, freq: 120 },
    { token: 'beta', n: 1, score: 3.1, freq: 118 },
    { token: 'gamma', n: 1, score: 2.9, freq: 110 },
    { token: 'epsilon', n: 1, score: 2.5, freq: 105 },
  ];

  const first = buildTopViews(sorted, 5);
  const second = buildTopViews(sorted, 5);

  assert.deepEqual(first, second, 'repeated builds over the same input remain deterministic');
  assert.deepEqual(
    first.phrases.map((entry) => entry.token),
    ['alpha beta gamma', 'alpha beta', 'delta epsilon', 'theta iota', 'alpha'],
    'phrases lead and only fall back to unigrams when needed',
  );
  const expectedWords = sorted.filter((entry) => entry.n === 1).map((entry) => entry.token).slice(0, 5);
  assert.deepEqual(
    first.words.map((entry) => entry.token),
    expectedWords,
    'unigram view contains only word candidates and is independent from phrases',
  );
  assert.equal(first.words.length, expectedWords.length, 'word view stops once available unigrams are exhausted');
  assert.ok(first.words.every((entry) => entry.n === 1), 'word view entries are unigrams');
});

test('reconcileTopViews enforces stable ordering with limited swap tolerance', () => {
  const basePhrases = [
    { token: 'alpha beta', score: 12, freq: 80, n: 2 },
    { token: 'beta gamma', score: 11, freq: 70, n: 2 },
    { token: 'gamma delta', score: 10, freq: 68, n: 2 },
  ];
  const baseWords = [
    { token: 'alpha', score: 3, freq: 140, n: 1 },
    { token: 'beta', score: 2.8, freq: 130, n: 1 },
    { token: 'gamma', score: 2.7, freq: 128, n: 1 },
  ];

  const prev = { phrases: basePhrases, words: baseWords };

  const oneSwap = {
    phrases: [
      { token: 'beta gamma', score: 11, freq: 70, n: 2 },
      { token: 'alpha beta', score: 12, freq: 80, n: 2 },
      { token: 'gamma delta', score: 10, freq: 68, n: 2 },
    ],
    words: baseWords.map((entry) => ({ ...entry })),
  };
  const accepted = reconcileTopViews(prev, oneSwap, { maxSwaps: 1 });
  assert.deepEqual(
    accepted.phrases.map((entry) => entry.token),
    ['beta gamma', 'alpha beta', 'gamma delta'],
    'single adjacent swap is permitted',
  );

  const multiSwap = {
    phrases: [
      { token: 'gamma delta', score: 10, freq: 68, n: 2 },
      { token: 'alpha beta', score: 12, freq: 80, n: 2 },
      { token: 'beta gamma', score: 11, freq: 70, n: 2 },
    ],
    words: baseWords.map((entry) => ({ ...entry })),
  };
  const rejected = reconcileTopViews(prev, multiSwap, { maxSwaps: 1 });
  assert.deepEqual(
    rejected.phrases.map((entry) => entry.token),
    ['alpha beta', 'beta gamma', 'gamma delta'],
    'orders requiring more than one swap fall back to the previous view',
  );

  const metricShift = {
    phrases: [
      { token: 'gamma delta', score: 15, freq: 90, n: 2 },
      { token: 'beta gamma', score: 12, freq: 80, n: 2 },
      { token: 'alpha beta', score: 6, freq: 60, n: 2 },
    ],
    words: baseWords.map((entry) => ({ ...entry })),
  };
  const acceptedShift = reconcileTopViews(prev, metricShift, { maxSwaps: 1 });
  assert.deepEqual(
    acceptedShift.phrases.map((entry) => entry.token),
    ['gamma delta', 'beta gamma', 'alpha beta'],
    'significant metric changes allow new ordering even if swaps exceed tolerance',
  );

  const newTokens = {
    phrases: [
      { token: 'delta epsilon', score: 9, freq: 60, n: 2 },
      { token: 'epsilon zeta', score: 8, freq: 55, n: 2 },
      { token: 'zeta eta', score: 7, freq: 50, n: 2 },
    ],
    words: baseWords.map((entry) => ({ ...entry })),
  };
  const replaced = reconcileTopViews(prev, newTokens, { maxSwaps: 1 });
  assert.deepEqual(
    replaced.phrases.map((entry) => entry.token),
    ['delta epsilon', 'epsilon zeta', 'zeta eta'],
    'entirely new token sets supersede the previous ordering',
  );
  assert.deepEqual(
    replaced.words.map((entry) => entry.token),
    ['alpha', 'beta', 'gamma'],
    'word view remains intact when unchanged',
  );
  assert.deepEqual(
    prev.phrases.map((entry) => entry.token),
    ['alpha beta', 'beta gamma', 'gamma delta'],
    'previous snapshot is not mutated during reconciliation',
  );
});
