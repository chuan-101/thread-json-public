import test from 'node:test';
import assert from 'node:assert/strict';

import { threeMonthCutoff, filterMessagesByWindow } from '../src/stats/models.js';
import { resetYearAgg, bumpYearAgg, finalizeYearAgg } from '../src/stats/timeline.js';
import { mergePartials } from '../src/stats/merge.js';

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

test('mergePartials keeps results stable across runs', () => {
  const cmsTable = new Uint32Array(8).fill(50);
  const cms = { depth: 2, width: 4, table: cmsTable };
  const partials = [
    {
      mgTopK: [
        { token: 'alpha', countEst: 5 },
        { token: 'beta', countEst: 7 },
        { token: 'gamma', countEst: 2 },
        { token: 'delta', countEst: 1 },
      ],
      totalTokens: 20,
      cms,
    },
    {
      mgTopK: [
        { token: 'beta', countEst: 4 },
        { token: 'delta', countEst: 10 },
        { token: 'epsilon', countEst: 6 },
      ],
      totalTokens: 30,
      cms,
    },
  ];

  const first = mergePartials(partials, 3);
  const second = mergePartials(partials, 3);

  assert.deepEqual(first, second, 'repeated runs should be deterministic');
  assert.deepEqual(
    first,
    { candidates: ['beta', 'delta', 'epsilon'], totalTokens: 50 },
    'top candidates preserve deterministic ordering and token totals',
  );
});
