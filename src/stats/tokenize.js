export const FIXED_STOPWORDS = new Set([
  // Chinese fillers & particles
  '啊', '呀', '嘛', '呢', '吧', '的', '了', '着', '就', '还', '也', '而且', '然后', '就是', '以及',
  // Pronouns (keep minimal to avoid over-filtering)
  '你', '我', '我们', '他们', '她们', '它们',
  // English common
  'the', 'a', 'an', 'and', 'or', 'of', 'to', 'in', 'on', 'for', 'with', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
  'that', 'this', 'it', 'as', 'at', 'by', 'from',
]);

export const ALIAS = new Map([
  ['chat_gpt', 'chatgpt'],
  ['open_ai', 'openai'],
  ['gpt_4', 'gpt4'],
  ['gpt_3', 'gpt3'],
]);

export const WHITELIST = new Set([
  'chatgpt',
  'openai',
  'gpt4',
  'gpt3',
]);

const CJK_CLASS = '\\p{Script=Han}\\p{Script=Hiragana}\\p{Script=Katakana}\\p{Script=Hangul}';
const TOKEN_REGEX = new RegExp(`[${CJK_CLASS}]|[a-z0-9_]+`, 'gu');
const CJK_TOKEN_REGEX = new RegExp(`^[${CJK_CLASS}]+$`, 'u');
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
    let token = match[0];
    if (!token) continue;

    token = ALIAS.get(token) || token;

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

export function inferTokenScript(token) {
  if (typeof token !== 'string' || token.length === 0) {
    return 'other';
  }
  if (LATIN_REGEX.test(token)) {
    return 'latin';
  }
  if (CJK_TOKEN_REGEX.test(token)) {
    return 'cjk';
  }
  return 'other';
}
