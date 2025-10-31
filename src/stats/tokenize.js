export const FIXED_STOPWORDS = new Set([
  // Chinese fillers & particles
  '啊', '呀', '嘛', '呢', '吧', '的', '了', '着', '就', '还', '也', '而且', '然后', '就是', '以及',
  // Pronouns (keep minimal to avoid over-filtering)
  '你', '我', '我们', '他们', '她们', '它们',
  // English common
  'the', 'a', 'an', 'and', 'or', 'of', 'to', 'in', 'on', 'for', 'with', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
  'that', 'this', 'it', 'as', 'at', 'by', 'from',
]);

const CJK_CLASS = '\\p{Script=Han}\\p{Script=Hiragana}\\p{Script=Katakana}\\p{Script=Hangul}';
const TOKEN_REGEX = new RegExp(`[${CJK_CLASS}]|[a-z0-9_]+`, 'gu');
const LATIN_REGEX = /^[a-z0-9_]+$/;
const DIGIT_REGEX = /^\d+$/;
const SINGLE_LATIN_REGEX = /^[a-z_]$/;

function resolveStopwords() {
  return FIXED_STOPWORDS;
}

export function* tokenizeIter(text) {
  if (!text) return;
  const stopwords = resolveStopwords();
  const lower = String(text).toLowerCase();

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
    yield token;
  }
}

export function tokenize(text) {
  if (!text) return [];
  return Array.from(tokenizeIter(text));
}
