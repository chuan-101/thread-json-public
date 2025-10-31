import test from 'node:test';
import assert from 'node:assert/strict';

import { threeMonthCutoff, filterMessagesByWindow } from '../src/stats/models.js';
import { resetYearAgg, bumpYearAgg, finalizeYearAgg } from '../src/stats/timeline.js';
import { mergePartials, scoreCandidates } from '../src/stats/merge.js';
import { tokenize, WHITELIST } from '../src/stats/tokenize.js';

const DAY_MS = 24 * 3600 * 1000;

test('threeMonthCutoff moves one day when clock shifts', () => {
  const baseNow = Date.UTC(2024, 5, 1);
  const cutoff = threeMonthCutoff(baseNow);
  const forwardCutoff = threeMonthCutoff(baseNow + DAY_MS);
  const backwardCutoff = threeMonthCutoff(baseNow - DAY_MS);

  assert.equal(forwardCutoff - cutoff, DAY_MS, 'forward shift should advance cutoff by one day');
  assert.equal(cutoff - backwardCutoff, DAY_MS, 'backward shift should rewind cutoff by one day');

  const nearCutoffMsg = { ts: cutoff - 1000 };
  assert.equal(filterMessagesByWindow([nearCutoffMsg], cutoff).length, 0, 'message just before cutoff excluded');
  assert.equal(
    filterMessagesByWindow([nearCutoffMsg], backwardCutoff).length,
    1,
    'rolling backward should include recent-enough message',
  );
  assert.equal(
    filterMessagesByWindow([nearCutoffMsg], forwardCutoff).length,
    0,
    'rolling forward tightens window and still excludes message',
  );
});

test('yearly overview aggregates characters, images, and streaks', () => {
  resetYearAgg();
  const jan1 = Date.UTC(2023, 0, 1);
  const jan2 = Date.UTC(2023, 0, 2);
  const jan4 = Date.UTC(2023, 0, 4);

  bumpYearAgg(jan1, 'assistant', '你好', 2);
  bumpYearAgg(jan2, 'user', 'a\r\nb', 0);
  bumpYearAgg(jan4, 'assistant', '123', 1);

  const summary = finalizeYearAgg();
  assert.ok(summary[2023], 'year entry should exist');
  const stats2023 = summary[2023];

  assert.equal(stats2023.chars, 8, 'character count includes all roles with newline normalization');
  assert.equal(stats2023.images, 3, 'image totals accumulate per message');
  assert.equal(stats2023.activeDays, 3, 'active days track distinct calendar days');
  assert.equal(stats2023.streakCount, 2, 'streak segments are computed per year');
  assert.equal(stats2023.longestStreak, 2, 'longest streak spans consecutive days');

  resetYearAgg();
  assert.deepEqual(finalizeYearAgg(), {}, 'reset clears accumulator state');
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
