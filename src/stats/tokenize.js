export const FIXED_STOPWORDS = new Set([
  // Chinese fillers & particles
  '啊', '呀', '嘛', '呢', '吧', '的', '了', '着', '就', '还', '也', '而且', '然后', '就是', '以及',
  // Pronouns (keep minimal to avoid over-filtering)
  '你', '我', '我们', '他们', '她们', '它们',
  '一边','现在','什么','还是','如果','因为','所以',
  '这样','这个','那个','以及','比如','还有','是否','可能','需要','可以','已经','应该','并且',
  '然后','其实','不过','只是','或者','而且','同时','也许','大概','比较',
  '但是','不过','而且','同时','然后','于是',
  '其实','反而','只是','完全','基本','差不多',
  '一定','肯定','可能','大概','大约','多半','大多',
  '有点','有些','稍微','比较','特别','非常','挺',
  '还是','依然','依旧',
  '的话','的话呢','的时候','这种','那种','这种情况','那种情况',
  '总之','反正','比如说','比如讲','例如',
  '一会','一会儿','一下','一下子','一阵','一阵子',
  '刚刚','刚才','后来','之前','之后','后来','以后',
  '其实','老实说','说实话',
  '这里','那边','那里','这边',
  '每次','有时候','有时','经常','常常',
  '觉得','感觉','认为',
  '需要','可能会','应该','好像',
  '知道','明白','理解',
  '一下下','稍稍','有点儿',
  '反复','不断','一直',
  // English common
  'the', 'a', 'an', 'and', 'or', 'of', 'to', 'in', 'on', 'for', 'with', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
  'that', 'this', 'it', 'as', 'at', 'by', 'from','now','what','which','when','why','how',
  'just','really','quite','maybe','perhaps',
  'very','so','too','also','then','well',
  'kind','sort','kinda','sorta',
  'ever','never','always','often','sometimes',
  'really','actually','basically','literally'
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

let cjkStopwordRegex = null;

function resolveStopwords() {
  return FIXED_STOPWORDS;
}

function resolveCjkStopwordRegex() {
  if (cjkStopwordRegex !== null) return cjkStopwordRegex;

  const cjkStopwords = Array.from(FIXED_STOPWORDS).filter((token) => CJK_TOKEN_REGEX.test(token));
  if (!cjkStopwords.length) {
    cjkStopwordRegex = null;
    return cjkStopwordRegex;
  }
  
    cjkStopwords.sort((a, b) => b.length - a.length); 

  const escaped = cjkStopwords.map((token) => token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'));
  cjkStopwordRegex = new RegExp(escaped.join('|'), 'gu');
  return cjkStopwordRegex;
}

export function* tokenizeIter(text) {
  if (!text) return;
  const stopwords = resolveStopwords();
  const lower = String(text).toLowerCase();
  const cjkRegex = resolveCjkStopwordRegex();
  if (cjkRegex) {
    cjkRegex.lastIndex = 0;
  }
  const sanitized = cjkRegex ? lower.replace(cjkRegex, ' ') : lower;

  for (const match of sanitized.matchAll(TOKEN_REGEX)) {
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
