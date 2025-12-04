import { appendToShard, finalizeShard } from './db.js';
import { bumpYearAgg, resetYearAgg } from '../stats/timeline.js';
import { bumpModelBucket } from '../stats/models.js';

let shardTasks = [];

export async function parseJSONStream(
  file,
  onMessage,
  onProgress,
  opts = {}
) {
  if (!file || typeof file.stream !== 'function') {
    throw new Error('A File instance with stream() is required');
  }

  resetYearAgg();

  const assistName = (opts.assistName || '').trim();
  const totalBytes = typeof file.size === 'number' ? file.size : 0;
  const MIN_CHUNK = 1024 * 1024;
  const MAX_CHUNK = 4 * 1024 * 1024;
  const PREFERRED_CHUNK = 2 * 1024 * 1024;

  let stream = file.stream();
  let byob = false;
  let reader;
  try {
    reader = stream.getReader({ mode: 'byob' });
    byob = true;
  } catch (err) {
    stream = file.stream();
    reader = stream.getReader();
  }

  const decoder = new TextDecoder('utf-8');
  const requested = totalBytes ? Math.min(totalBytes, MAX_CHUNK) : PREFERRED_CHUNK;
  const chunkSize = Math.min(MAX_CHUNK, Math.max(MIN_CHUNK, requested, PREFERRED_CHUNK));
  let chunkBuffer = byob ? new Uint8Array(chunkSize) : null;

  let buffer = '';
  let bytesRead = 0;
  let rootDetected = false;
  let rootType = null; // 'array' | 'object'
  let parseIndex = 0; // current index in buffer for parsing
  let finished = false;
  let aborted = false;

  const signal = opts.signal;

  await Promise.allSettled(shardTasks);
  shardTasks.length = 0;

  const abortHandler = () => {
    if (aborted) return;
    aborted = true;
    try {
      reader?.cancel?.();
    } catch (err) {
      // ignore cancellation failures
    }
  };

  const progressState = {
    parsePct: 0,
    shardPct: 0,
    statsPct: undefined,
  };

  let bytesPersisted = 0;

  const updateProgress = (patch = {}) => {
    if (typeof onProgress !== 'function') return;
    if (typeof patch.parsePct === 'number') {
      progressState.parsePct = patch.parsePct;
    }
    if (typeof patch.shardPct === 'number') {
      progressState.shardPct = patch.shardPct;
    }
    if (typeof patch.statsPct === 'number') {
      progressState.statsPct = patch.statsPct;
    }
    onProgress({
      parsePct: progressState.parsePct,
      shardPct: progressState.shardPct,
      statsPct: progressState.statsPct,
    });
  };

  const normalizeTs = (raw) => {
    if (raw == null) return undefined;
    let num = Number(raw);
    if (!Number.isFinite(num)) return undefined;
    if (num <= 0) return undefined;
    if (num < 1e12) {
      // assume seconds
      num = Math.round(num * 1000);
    } else if (num > 1e15) {
      // maybe in microseconds
      num = Math.round(num / 1000);
    }
    return num;
  };

  const normalizeRole = (msg) => {
  if (!msg || typeof msg !== 'object') return null;
  const rawRole = msg.role || msg.author?.role || msg.author;
  if (!rawRole || typeof rawRole !== 'string') return null;
  const lower = rawRole.toLowerCase().trim();
  if (lower === 'user' || lower === 'human') return 'user';
  if (lower === 'assistant' || lower === 'gpt' || lower === 'chatgpt' || lower === 'model') return 'assistant';
  if (lower === 'system') return 'system';
  return null;
};

const normalizeContent = (msg) => {
  const texts = [];
  
  const pushText = (val) => {
    if (typeof val === 'string' && val) {
      texts.push(val);
    }
  };
  
  const { content, parts } = msg || {};
  
  // 处理 content 为 string
  if (typeof content === 'string') {
    pushText(content);
  }
  // 处理 content 为 array
  else if (Array.isArray(content)) {
    for (const part of content) {
      if (typeof part === 'string') {
        pushText(part);
      } else if (part && typeof part === 'object') {
        if (typeof part.text === 'string') {
          pushText(part.text);
        } else if (part.type === 'text') {
          if (typeof part.text === 'string') {
            pushText(part.text);
          } else if (part.text && typeof part.text.value === 'string') {
            pushText(part.text.value);
          }
        }
      }
    }
  }
  
  // 处理 parts array
  if (Array.isArray(parts)) {
    for (const part of parts) {
      if (typeof part === 'string') {
        pushText(part);
      } else if (part && typeof part === 'object' && typeof part.text === 'string') {
        pushText(part.text);
      }
    }
  }
  
  // fallback：如果还是没提取到内容，再试试 content.text
  if (texts.length === 0 && content && typeof content === 'object') {
    const maybeText = content.text;
    if (typeof maybeText === 'string') {
      pushText(maybeText);
    } else if (maybeText && typeof maybeText.value === 'string') {
      pushText(maybeText.value);
    }
    if (Array.isArray(content.parts)) {
      for (const part of content.parts) {
        if (typeof part === 'string') {
          pushText(part);
        } else if (part && typeof part === 'object') {
          if (typeof part.text === 'string') {
            pushText(part.text);
          } else if (part.text && typeof part.text.value === 'string') {
            pushText(part.text.value);
          }
        }
      }
    }
  }
  
  return texts.join('\n');
};

  const countContentChars = (content) => {
    if (content == null) return 0;
    const normalized = String(content).replace(/\r\n?/g, '\n');
    if (!normalized) return 0;
    return Array.from(normalized).length;
  };

  const registerImageKey = (seen, key) => {
    const strKey = typeof key === 'string' && key ? key : null;
    if (strKey) {
      if (seen.has(strKey)) return false;
      seen.add(strKey);
    }
    return true;
  };

  const countImagesInMessage = (msg, normalizedContent) => {
    if (!msg || typeof msg !== 'object') return 0;
    const seen = new Set();
    let total = 0;

    const maybeRecord = (key) => {
      if (registerImageKey(seen, key)) {
        total += 1;
      }
    };

    const looksLikeImageMime = (value) => {
      if (typeof value !== 'string') return false;
      return value.toLowerCase().startsWith('image/');
    };

    const inspectAttachment = (attachment) => {
      if (!attachment || typeof attachment !== 'object') return;
      const type = typeof attachment.type === 'string' ? attachment.type.toLowerCase() : '';
      const category = typeof attachment.category === 'string' ? attachment.category.toLowerCase() : '';
      const purpose = typeof attachment.purpose === 'string' ? attachment.purpose.toLowerCase() : '';
      const mimeCandidates = [
        attachment.mime_type,
        attachment.content_type,
        attachment.mime,
        attachment.media_type,
        attachment.file?.mime_type,
        attachment.file?.content_type,
        attachment.file?.media_type,
      ];
      const hasImageMime = mimeCandidates.some(looksLikeImageMime);
      const looksLikeImageType =
        type.includes('image') || category.includes('image') || purpose.includes('image');
      if (hasImageMime || looksLikeImageType) {
        maybeRecord(
          attachment.id ||
            attachment.file_id ||
            attachment.file?.id ||
            attachment.url ||
            attachment.file?.url ||
            attachment.file?.name ||
            `${type}:${attachment.name || attachment.filename || ''}`,
        );
        return;
      }
      const url =
        attachment.url ||
        attachment.href ||
        attachment.link ||
        attachment.file?.url ||
        attachment.source ||
        attachment.download_url;
      if (typeof url === 'string') {
        const lower = url.toLowerCase();
        if (/\.(png|jpe?g|gif|webp|bmp|svg|heic|heif|tiff?|avif)(\?|#|$)/.test(lower)) {
          maybeRecord(url);
        }
      }
    };

    const visitList = (list) => {
      if (!Array.isArray(list)) return;
      list.forEach((item) => {
        if (!item || typeof item !== 'object') return;
        inspectAttachment(item);
        if (item.file && typeof item.file === 'object') {
          inspectAttachment(item.file);
        }
      });
    };

    visitList(msg.attachments);
    visitList(msg.files);
    visitList(msg.assets);
    visitList(msg.metadata?.attachments);
    visitList(msg.metadata?.files);

    const considerContentParts = (parts) => {
      if (!Array.isArray(parts)) return;
      parts.forEach((part) => {
        if (!part || typeof part !== 'object') return;
        const type = typeof part.type === 'string' ? part.type.toLowerCase() : '';
        if (type.includes('image')) {
          if (type === 'image_url' && part.image_url) {
            if (typeof part.image_url === 'string') {
              maybeRecord(part.image_url);
            } else if (part.image_url && typeof part.image_url === 'object') {
              maybeRecord(part.image_url.url || part.image_url.href || part.image_url.id);
            }
          } else {
            maybeRecord(
              part.id ||
                part.asset_pointer ||
                part.file_id ||
                part.url ||
                `${type}:${part.media_type || part.mime_type || ''}`,
            );
          }
          return;
        }
        if (looksLikeImageMime(part.media_type) || looksLikeImageMime(part.mime_type)) {
          maybeRecord(part.id || part.asset_pointer || `${part.media_type}:${part.url || ''}`);
        }
        if (Array.isArray(part.parts)) {
          considerContentParts(part.parts);
        }
      });
    };

    considerContentParts(Array.isArray(msg.content) ? msg.content : undefined);
    if (msg.content && typeof msg.content === 'object' && Array.isArray(msg.content.parts)) {
      considerContentParts(msg.content.parts);
    }
    if (Array.isArray(msg.parts)) {
      considerContentParts(msg.parts);
    }

    if (typeof normalizedContent === 'string' && normalizedContent) {
      const regex = /!\[[^\]]*\]\(([^)]+)\)/g;
      let match;
      while ((match = regex.exec(normalizedContent)) !== null) {
        maybeRecord(match[1]);
      }
    }

    return total;
  };

const pickModel = (msg) => {
  if (!msg || typeof msg !== 'object') return undefined;
  const meta = msg.metadata || {};
  return (
    msg.model ||
    msg.model_slug ||
    meta.model_slug ||
    meta.default_model_slug ||
    meta.model ||
    msg.recipient ||
    undefined
  ) || undefined;
};

  const shouldEmit = (msg) => {
    if (!msg || typeof msg !== 'object') return false;
    const role = normalizeRole(msg);
    if (role !== 'assistant') return false;
    if (assistName && (msg.name || msg.author?.name || '') !== assistName) {
      return false;
    }
    return true;
  };

  const appendShardSlice = (message) => {
    if (!message || aborted) return;
    appendToShard(message).catch((err) => {
      console.error('Failed to append shard slice', err);
    });
  };

  const processMessage = (msg, convTs) => {
    if (!msg || typeof msg !== 'object') return;
    const role = normalizeRole(msg);
    const ts =
      normalizeTs(msg.create_time || msg.update_time || msg.end_turn?.time || msg.ts) ||
      convTs;
    const model = pickModel(msg);
    const content = normalizeContent(msg);
    const contentChars = countContentChars(content);
    const imagesInMsg = countImagesInMessage(msg, content);

    if (Number.isFinite(ts)) {
      bumpYearAgg(ts, role || 'unknown', contentChars, imagesInMsg);
    }

    bumpModelBucket(ts, role, { text: content, model });

    if (aborted || !shouldEmit(msg)) return;

    const payload = {
      role: 'assistant',
      name: msg.name || msg.author?.name || undefined,
      content,
      model,
      ts,
    };
    payload.content = payload.content == null ? '' : String(payload.content);
    payload.model = payload.model == null ? undefined : String(payload.model);
    if (typeof onMessage === 'function') {
      onMessage(payload);
    }
    appendShardSlice(payload);
  };

  const processConversation = (conv) => {
    if (!conv || typeof conv !== 'object') return;
    const convTs =
      normalizeTs(conv.create_time || conv.update_time || conv.timestamp) ||
      normalizeTs(conv.current_node?.message?.create_time);

    if (Array.isArray(conv.messages)) {
      conv.messages.forEach((msgLike) => {
        if (!msgLike) return;
        const msg = msgLike.message || msgLike;
        processMessage(msg, convTs);
      });
    }

    if (conv.mapping && typeof conv.mapping === 'object') {
      Object.values(conv.mapping).forEach((node) => {
        if (node && typeof node === 'object') {
          const msg = node.message || node.data?.message || node;
          processMessage(msg, convTs);
          if (Array.isArray(node.children)) {
            node.children.forEach((childId) => {
              // children are references; handled when visited by mapping values
            });
          }
        }
      });
    }

    if (conv.items && Array.isArray(conv.items)) {
      conv.items.forEach((item) => {
        const msg = item.message || item.data || item;
        processMessage(msg, convTs);
      });
    }

    if (conv.messages == null && conv.mapping == null && conv.items == null) {
      // fallback: try to walk generic arrays/objects for message-like entries
      const scan = (value) => {
        if (!value) return;
        if (Array.isArray(value)) {
          value.forEach(scan);
          return;
        }
        if (typeof value === 'object') {
          if (value.role || value.author?.role) {
            processMessage(value, convTs);
            return;
          }
          Object.values(value).forEach(scan);
        }
      };
      scan(conv);
    }
  };

  const skipWhitespace = (str, start) => {
    let i = start;
    while (i < str.length) {
      const ch = str.charCodeAt(i);
      if (ch === 0xfeff || ch === 0x20 || ch === 0x0a || ch === 0x0d || ch === 0x09) {
        i += 1;
      } else {
        break;
      }
    }
    return i;
  };

  const findValueEnd = (str, start) => {
    if (start >= str.length) return -1;
    const first = str[start];
    if (first === '"') {
      let i = start + 1;
      let escaped = false;
      while (i < str.length) {
        const ch = str[i];
        if (escaped) {
          escaped = false;
        } else if (ch === '\\') {
          escaped = true;
        } else if (ch === '"') {
          return i + 1;
        }
        i += 1;
      }
      return -1;
    }
    if (first === '{' || first === '[') {
      const stack = [];
      let inString = false;
      let escaped = false;
      for (let i = start; i < str.length; i += 1) {
        const ch = str[i];
        if (escaped) {
          escaped = false;
          continue;
        }
        if (ch === '\\') {
          if (inString) escaped = true;
          continue;
        }
        if (ch === '"') {
          inString = !inString;
          continue;
        }
        if (inString) continue;
        if (ch === '{' || ch === '[') {
          stack.push(ch);
        } else if (ch === '}' || ch === ']') {
          const last = stack.pop();
          if ((ch === '}' && last !== '{') || (ch === ']' && last !== '[')) {
            throw new Error('JSON bracket mismatch');
          }
          if (stack.length === 0) {
            return i + 1;
          }
        }
      }
      return -1;
    }
    // number / literal
    let i = start;
    while (i < str.length) {
      const ch = str[i];
      if (
        ch === ',' ||
        ch === ']' ||
        ch === '}' ||
        ch === '\n' ||
        ch === '\r' ||
        ch === '\t' ||
        ch === ' '
      ) {
        break;
      }
      i += 1;
    }
    return i;
  };

  const processBuffer = async (isFinal = false) => {
    let consumed = 0;
    let iterationCount = 0; 
    while (parseIndex < buffer.length) {
      if (iterationCount++ % 10 === 0) {  
        await new Promise(r => setTimeout(r, 0));  
    }
      if (!rootDetected) {
        const next = skipWhitespace(buffer, parseIndex);
        if (next >= buffer.length && !isFinal) break;
        if (next >= buffer.length) {
          parseIndex = next;
          break;
        }
        const ch = buffer[next];
        if (ch === '[') {
          rootType = 'array';
          rootDetected = true;
          parseIndex = next + 1;
          consumed = parseIndex;
          continue;
        }
        if (ch === '{') {
          rootType = 'object';
          rootDetected = true;
          parseIndex = next;
          continue;
        }
        parseIndex = next + 1;
        consumed = parseIndex;
        continue;
      }

      if (rootType === 'array') {
        const start = skipWhitespace(buffer, parseIndex);
        if (start >= buffer.length) break;
        const ch = buffer[start];
        if (ch === ',') {
          parseIndex = start + 1;
          consumed = parseIndex;
          continue;
        }
        if (ch === ']') {
          parseIndex = start + 1;
          consumed = parseIndex;
          finished = true;
          break;
        }
        const end = findValueEnd(buffer, start);
        if (end === -1) break; // need more data
        const segment = buffer.slice(start, end);
        parseIndex = end;
        consumed = parseIndex;
        try {
          const conv = JSON.parse(segment);
          processConversation(conv);
        } catch (err) {
          console.error('Failed to parse segment', err);
        }
        continue;
      }

      if (rootType === 'object') {
        const end = findValueEnd(buffer, parseIndex);
        if (end === -1) break;
        const segment = buffer.slice(parseIndex, end);
        parseIndex = end;
        consumed = parseIndex;
        try {
          const conv = JSON.parse(segment);
          processConversation(conv);
        } catch (err) {
          // Try to parse the object later by buffering more data.
          if (!isFinal) {
            parseIndex -= segment.length;
            break;
          }
        }
        finished = true;
        break;
      }
      break;
    }

    if (consumed > 0) {
      buffer = buffer.slice(consumed);
      parseIndex -= consumed;
      if (parseIndex < 0) parseIndex = 0;
    }
  };

  if (signal?.aborted) {
    abortHandler();
  } else if (signal) {
    signal.addEventListener('abort', abortHandler, { once: true });
  }

  let readError = null;

while (!aborted) {
  let readChunk;
  let done = false;
  try {
    if (byob) {
      if (!chunkBuffer || chunkBuffer.byteLength === 0) {
        chunkBuffer = new Uint8Array(chunkSize);
      }
      const { value, done: readerDone } = await reader.read(chunkBuffer);
      readChunk = value;
      done = readerDone;
      chunkBuffer = new Uint8Array(chunkSize);
    } else {
      const res = await reader.read();
      readChunk = res.value;
      done = res.done;
    }
  } catch (err) {
    if (err?.name === 'AbortError') {
        aborted = true;
      } else {
        readError = err;
      }
      break;
    }

    if (signal?.aborted) {
      abortHandler();
      break;
    }

    if (readChunk && readChunk.byteLength) {
      bytesRead += readChunk.byteLength;
      buffer += decoder.decode(readChunk, { stream: true });
      await processBuffer(false);
      const pct = totalBytes ? Math.min(100, (bytesRead / totalBytes) * 100) : 0;
      updateProgress({ parsePct: pct });
    }

    if (done) break;
  }

  reader.releaseLock?.();

  signal?.removeEventListener?.('abort', abortHandler);

  if (readError) {
    throw readError;
  }

  if (!aborted) {
    buffer += decoder.decode();
    await processBuffer(true);
    finished = true;
    await Promise.allSettled(shardTasks);
    shardTasks.length = 0;
    try {
      await finalizeShard();
    } catch (err) {
      console.error('Failed to finalize shard', err);
    }
    updateProgress({ parsePct: 100, shardPct: 100 });
  } else {
    await Promise.allSettled(shardTasks);
    shardTasks.length = 0;
  }

  return { finished, aborted };
}
