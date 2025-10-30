const BUILT_IN_STOPWORDS = [
  // English fillers
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'been', 'but', 'by', 'for', 'from', 'had', 'has', 'have', 'he', 'her',
  'hers', 'him', 'his', 'how', 'i', 'if', 'in', 'is', 'it', 'its', 'me', 'my', 'of', 'on', 'or', 'ours', 'she', 'so',
  'that', 'the', 'their', 'them', 'then', 'there', 'these', 'they', 'this', 'to', 'too', 'us', 'was', 'we', 'were',
  'what', 'when', 'where', 'which', 'who', 'why', 'with', 'you', 'your',
  // Common chat fillers
  'okay', 'ok', 'yeah', 'yep', 'hey', 'hi', 'hello', 'thanks', 'thank', 'please',
  // Simplified Chinese stopwords (light)
  '的', '了', '是', '我', '你', '他', '她', '它', '们', '和', '啊', '吧', '吗', '呢', '就', '在', '还', '很', '都', '要', '会',
  '有', '也', '对', '着', '把', '给', '个', '再', '让', '又', '被', '去', '来', '好', '跟', '用', '于',
];

const STOPWORD_SPLIT_REGEX = /[\s,;\u3000\uFF0C\uFF1B]+/u;
const CJK_CLASS = '\\p{Script=Han}\\p{Script=Hiragana}\\p{Script=Katakana}\\p{Script=Hangul}';
const TOKEN_REGEX = new RegExp(`[${CJK_CLASS}]|[a-z0-9_]+`, 'gu');
const LATIN_REGEX = /^[a-z0-9_]+$/;
const DIGIT_REGEX = /^\d+$/;
const SINGLE_LATIN_REGEX = /^[a-z_]$/;

function normalizeStopwordEntry(entry) {
  if (typeof entry !== 'string') return null;
  const trimmed = entry.trim().toLowerCase();
  return trimmed ? trimmed : null;
}

export function createStopwordSet(extra = []) {
  const set = new Set(BUILT_IN_STOPWORDS);
  const addEntry = (value) => {
    const normalized = normalizeStopwordEntry(value);
    if (normalized) {
      set.add(normalized);
    }
  };

  if (extra instanceof Set) {
    extra.forEach((value) => addEntry(value));
  } else if (Array.isArray(extra)) {
    extra.forEach((value) => addEntry(value));
  } else if (typeof extra === 'string') {
    extra.split(STOPWORD_SPLIT_REGEX).forEach((value) => addEntry(value));
  } else if (extra && typeof extra === 'object') {
    Object.values(extra).forEach((value) => addEntry(value));
  }

  return set;
}

export function tokenize(text, stopwordSet = createStopwordSet()) {
  if (!text) return [];
  const stopwords = stopwordSet instanceof Set ? stopwordSet : createStopwordSet(stopwordSet);
  const lower = String(text).toLowerCase();
  const tokens = [];

  for (const match of lower.matchAll(TOKEN_REGEX)) {
    const token = match[0];
    if (!token) continue;
    if (stopwords.has(token)) {
      continue;
    }
    if (LATIN_REGEX.test(token)) {
      if (SINGLE_LATIN_REGEX.test(token)) {
        continue; // drop single-character latin tokens
      }
      if (DIGIT_REGEX.test(token) && token.length > 6) {
        continue; // drop long numeric sequences
      }
    }
    tokens.push(token);
  }

  return tokens;
}

export { BUILT_IN_STOPWORDS };
