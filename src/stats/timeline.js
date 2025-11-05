const YEAR_STATE = new Map();

function ensureYearEntry(year) {
  let entry = YEAR_STATE.get(year);
  if (!entry) {
    entry = {
      totalMessages: 0,
      totalChars: 0,
      assistantChars: 0,
      assistantMsgs: 0,
      charsByMonth: Array.from({ length: 12 }, () => 0),
      images: 0,
      activeDays: new Set(),
    };
    YEAR_STATE.set(year, entry);
  }
  return entry;
}

export function resetYearAgg() {
  YEAR_STATE.clear();
}

export function bumpYearAgg(ts, role, contentLen, imagesInMsg = 0) {
  const timestamp = Number(ts);
  if (!Number.isFinite(timestamp)) {
    return;
  }
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return;
  }
  const year = date.getFullYear();
  const monthIndex = date.getMonth();
  const month = `${monthIndex + 1}`.padStart(2, '0');
  const day = `${date.getDate()}`.padStart(2, '0');
  const dayKey = `${year}-${month}-${day}`;

  const entry = ensureYearEntry(year);
  entry.totalMessages += 1;

  const length = Number.isFinite(contentLen) ? contentLen : 0;
  entry.totalChars += length;
  if (monthIndex >= 0 && monthIndex < entry.charsByMonth.length) {
    entry.charsByMonth[monthIndex] += length;
  }

  if (role === 'assistant') {
    entry.assistantMsgs += 1;
    entry.assistantChars += length;
  }

  if (imagesInMsg && Number.isFinite(imagesInMsg)) {
    entry.images += imagesInMsg;
  }
  entry.activeDays.add(dayKey);
}

export function finalizeYearAgg() {
  const result = {};
  for (const [year, entry] of YEAR_STATE.entries()) {
    result[year] = {
      totalMessages: entry.totalMessages,
      totalChars: entry.totalChars,
      assistantChars: entry.assistantChars,
      assistantMsgs: entry.assistantMsgs,
      charsByMonth: entry.charsByMonth.slice(),
      images: entry.images,
      activeDays: new Set(entry.activeDays),
    };
  }
  return result;
}
