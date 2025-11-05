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

function roundToSingleDecimal(value) {
  return Math.round(value * 10) / 10;
}

function normalizeActiveDays(activeDays) {
  if (activeDays instanceof Set) {
    return activeDays;
  }
  if (Array.isArray(activeDays)) {
    return new Set(activeDays);
  }
  return new Set();
}

function normalizeCharsByMonth(raw) {
  const months = Array.isArray(raw) ? raw : [];
  return Array.from({ length: 12 }, (_, idx) => {
    const value = Number(months[idx]);
    return Number.isFinite(value) ? value : 0;
  });
}

export function computeYearlyMetrics(yearAgg) {
  const result = {};
  if (!yearAgg) {
    return result;
  }

  const entries =
    yearAgg instanceof Map ? yearAgg.entries() : Object.entries(yearAgg);

  for (const [year, rawEntry] of entries) {
    if (!rawEntry || typeof rawEntry !== 'object') {
      continue;
    }

    const totalMessages = Number(rawEntry.totalMessages) || 0;
    const totalChars = Number(rawEntry.totalChars) || 0;
    const assistantChars = Number(rawEntry.assistantChars) || 0;
    const assistantMsgs = Number(rawEntry.assistantMsgs) || 0;
    const images = Number(rawEntry.images) || 0;

    const activeDays = normalizeActiveDays(rawEntry.activeDays);
    const activeDayCount = activeDays.size;

    const charsByMonth = normalizeCharsByMonth(rawEntry.charsByMonth);
    let mostActiveMonthIndex = -1;
    let mostChars = -Infinity;
    for (let idx = 0; idx < charsByMonth.length; idx += 1) {
      const chars = Number(charsByMonth[idx]) || 0;
      if (chars > mostChars) {
        mostChars = chars;
        mostActiveMonthIndex = idx;
      }
    }

    const avgCharsPerActiveDay =
      activeDayCount > 0 ? roundToSingleDecimal(totalChars / activeDayCount) : 0;
    const avgAssistantMsgLen =
      assistantMsgs > 0 ? roundToSingleDecimal(assistantChars / assistantMsgs) : 0;

    result[year] = {
      totalMessages,
      totalChars,
      assistantChars,
      assistantMsgs,
      charsByMonth: charsByMonth.slice(),
      images,
      activeDays: new Set(activeDays),
      messages: totalMessages,
      avgCharsPerActiveDay,
      mostActiveMonth: mostActiveMonthIndex >= 0 ? mostActiveMonthIndex + 1 : 0,
      avgAssistantMsgLen,
    };
  }

  return result;
}
