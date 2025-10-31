const YEAR_STATE = new Map();

function ensureYearEntry(year) {
  let entry = YEAR_STATE.get(year);
  if (!entry) {
    entry = {
      chars: 0,
      images: 0,
      activeDays: new Set(),
    };
    YEAR_STATE.set(year, entry);
  }
  return entry;
}

function normalizeContentForCount(content) {
  if (content == null) return '';
  return String(content).replace(/\r\n?/g, '\n');
}

function countChars(content) {
  const normalized = normalizeContentForCount(content);
  if (!normalized) return 0;
  // Array.from splits the string into user-perceived code points, so surrogate pairs count as 1.
  return Array.from(normalized).length;
}

export function resetYearAgg() {
  YEAR_STATE.clear();
}

export function bumpYearAgg(ts, _role, content, imagesInMsg = 0) {
  const timestamp = Number(ts);
  if (!Number.isFinite(timestamp)) {
    return;
  }
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return;
  }
  const year = date.getFullYear();
  const month = `${date.getMonth() + 1}`.padStart(2, '0');
  const day = `${date.getDate()}`.padStart(2, '0');
  const dayKey = `${year}-${month}-${day}`;

  const entry = ensureYearEntry(year);
  entry.chars += countChars(content);
  if (imagesInMsg && Number.isFinite(imagesInMsg)) {
    entry.images += imagesInMsg;
  }
  entry.activeDays.add(dayKey);
}

function computeStreaks(activeDaysSet) {
  if (!activeDaysSet || !activeDaysSet.size) {
    return { streakCount: 0, longestStreak: 0 };
  }
  const sortedDays = Array.from(activeDaysSet).sort();
  let streakCount = 0;
  let longestStreak = 0;
  let currentStreak = 0;
  let prevIndex = null;

  for (const key of sortedDays) {
    const [yearStr, monthStr, dayStr] = key.split('-');
    const year = Number(yearStr);
    const monthIndex = Number(monthStr) - 1;
    const day = Number(dayStr);
    if (!Number.isFinite(year) || !Number.isFinite(monthIndex) || !Number.isFinite(day)) {
      continue;
    }
    const dayIndex = Math.floor(Date.UTC(year, monthIndex, day) / 86400000);
    if (prevIndex === null || dayIndex !== prevIndex + 1) {
      streakCount += 1;
      currentStreak = 1;
    } else {
      currentStreak += 1;
    }
    if (currentStreak > longestStreak) {
      longestStreak = currentStreak;
    }
    prevIndex = dayIndex;
  }

  if (streakCount === 0 && longestStreak > 0) {
    streakCount = 1;
  }

  return {
    streakCount,
    longestStreak,
  };
}

export function finalizeYearAgg() {
  const result = {};
  for (const [year, entry] of YEAR_STATE.entries()) {
    const { streakCount, longestStreak } = computeStreaks(entry.activeDays);
    result[year] = {
      chars: entry.chars,
      images: entry.images,
      streakCount,
      longestStreak,
      activeDays: entry.activeDays.size,
    };
  }
  return result;
}
