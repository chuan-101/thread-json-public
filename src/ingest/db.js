const DB_NAME = 'echo-json-shards';
const DB_VERSION = 1;
const STORE_NAME = 'shards';
const MIN_SHARD_BYTES = 12 * 1024 * 1024; // 12MB
const MAX_SHARD_BYTES = 16 * 1024 * 1024; // 16MB target upper bound

let dbPromise = null;
let initPromise = null;
let bufferEntries = [];
let bufferBytes = 0;
let bufferMinTs = null;
let bufferMaxTs = null;
let persistedBytes = 0;
let lastShardId = null;

function hasIndexedDB() {
  return typeof indexedDB !== 'undefined';
}

function wrapRequest(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function wrapTransaction(tx) {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}

async function ensureDb() {
  if (!hasIndexedDB()) return null;
  if (!dbPromise) {
    dbPromise = new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);
      request.onupgradeneeded = () => {
        const db = request.result;
        if (!db.objectStoreNames.contains(STORE_NAME)) {
          db.createObjectStore(STORE_NAME, { autoIncrement: true });
        }
      };
      request.onsuccess = () => {
        const db = request.result;
        db.onversionchange = () => {
          db.close();
          dbPromise = null;
          initPromise = null;
        };
        resolve(db);
      };
      request.onerror = () => reject(request.error);
    });
  }
  const db = await dbPromise;
  if (!db) return null;
  if (!initPromise) {
    initPromise = (async () => {
      try {
        const tx = db.transaction(STORE_NAME, 'readonly');
        const store = tx.objectStore(STORE_NAME);
        let total = 0;
        const cursorRequest = store.openCursor();
        await new Promise((resolve, reject) => {
          cursorRequest.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
              const value = cursor.value;
              if (value && typeof value.size === 'number') {
                total += value.size;
              }
              cursor.continue();
            } else {
              resolve();
            }
          };
          cursorRequest.onerror = () => reject(cursorRequest.error);
        });
        await wrapTransaction(tx);
        persistedBytes = total;
      } catch (err) {
        // Ignore failures when computing persisted size.
      }
    })();
  }
  await initPromise;
  return db;
}

function resetBuffer() {
  bufferEntries = [];
  bufferBytes = 0;
  bufferMinTs = null;
  bufferMaxTs = null;
}

async function flushBuffer(db, force = false) {
  if (!bufferBytes) return lastShardId;
  if (!force && bufferBytes < MIN_SHARD_BYTES) return lastShardId;
  if (!db) {
    resetBuffer();
    return lastShardId;
  }

  const recomputeBufferBounds = () => {
    bufferMinTs = null;
    bufferMaxTs = null;
    for (const entry of bufferEntries) {
      const ts = typeof entry.ts === 'number' && Number.isFinite(entry.ts) ? entry.ts : null;
      if (ts == null) continue;
      bufferMinTs = bufferMinTs == null ? ts : Math.min(bufferMinTs, ts);
      bufferMaxTs = bufferMaxTs == null ? ts : Math.max(bufferMaxTs, ts);
    }
  };

  let flushedId = lastShardId;
  while (bufferEntries.length && (force || bufferBytes >= MIN_SHARD_BYTES)) {
    const limit = force ? Number.POSITIVE_INFINITY : Math.min(bufferBytes, MAX_SHARD_BYTES);
    const chunkEntries = [];
    let chunkBytes = 0;
    let chunkMinTs = null;
    let chunkMaxTs = null;

    for (let i = 0; i < bufferEntries.length; i += 1) {
      const entry = bufferEntries[i];
      const nextBytes = chunkBytes + entry.bytes;
      if (!force && chunkEntries.length && nextBytes > limit) {
        break;
      }
      chunkEntries.push(entry);
      chunkBytes = nextBytes;
      const ts = typeof entry.ts === 'number' && Number.isFinite(entry.ts) ? entry.ts : null;
      if (ts != null) {
        chunkMinTs = chunkMinTs == null ? ts : Math.min(chunkMinTs, ts);
        chunkMaxTs = chunkMaxTs == null ? ts : Math.max(chunkMaxTs, ts);
      }
      if (!force && chunkBytes >= limit) {
        break;
      }
    }

    if (!chunkEntries.length) {
      break;
    }

    bufferEntries = bufferEntries.slice(chunkEntries.length);
    bufferBytes -= chunkBytes;
    recomputeBufferBounds();

    const storedMessages = chunkEntries.map(({ text, ts, model, role }) => ({
      text: typeof text === 'string' ? text : String(text || ''),
      ts: typeof ts === 'number' && Number.isFinite(ts) ? ts : null,
      model: typeof model === 'string' ? model : model == null ? null : String(model),
      role: typeof role === 'string' ? role : null,
    }));

    const tx = db.transaction(STORE_NAME, 'readwrite');
    const store = tx.objectStore(STORE_NAME);
    const record = {
      messages: storedMessages,
      size: chunkBytes,
      messageCount: storedMessages.length,
    };
    if (chunkMinTs != null) record.minTs = chunkMinTs;
    if (chunkMaxTs != null) record.maxTs = chunkMaxTs;
    const id = await wrapRequest(store.add(record));
    await wrapTransaction(tx);
    persistedBytes += chunkBytes;
    lastShardId = id;
    flushedId = id;

    if (!force && bufferBytes < MIN_SHARD_BYTES) {
      break;
    }
  }

  return flushedId;
}

export async function openDB() {
  return ensureDb();
}

export async function appendToShard(message) {
  if (!message) return lastShardId;
  const db = await ensureDb();
  if (!db) return lastShardId;
  const text = typeof message.content === 'string'
    ? message.content
    : typeof message.text === 'string'
    ? message.text
    : typeof message === 'string'
    ? message
    : '';
  const ts = typeof message.ts === 'number' && Number.isFinite(message.ts) ? message.ts : null;
  const model =
    typeof message.model === 'string'
      ? message.model
      : typeof message.model === 'number'
      ? String(message.model)
      : undefined;
  const role = typeof message.role === 'string' ? message.role : null;
  const entryBlob = new Blob([text, '\n'], { type: 'text/plain' });
  const entryBytes = entryBlob.size;
  bufferEntries.push({ text, ts, model: model || null, role, bytes: entryBytes });
  bufferBytes += entryBytes;
  if (ts != null) {
    bufferMinTs = bufferMinTs == null ? ts : Math.min(bufferMinTs, ts);
    bufferMaxTs = bufferMaxTs == null ? ts : Math.max(bufferMaxTs, ts);
  }
  if (bufferBytes >= MIN_SHARD_BYTES) {
    return flushBuffer(db, false);
  }
  return lastShardId;
}

export async function finalizeShard() {
  const db = await ensureDb();
  if (!db) {
    resetBuffer();
    return lastShardId;
  }
  return flushBuffer(db, true);
}

export async function listShards(options = {}) {
  const db = await ensureDb();
  if (!db) return [];
  const tx = db.transaction(STORE_NAME, 'readonly');
  const store = tx.objectStore(STORE_NAME);
  const results = [];
  await new Promise((resolve, reject) => {
    const request = store.openCursor();
    request.onsuccess = (event) => {
      const cursor = event.target.result;
      if (cursor) {
        const value = cursor.value;
        const minTs = typeof value?.minTs === 'number' && Number.isFinite(value.minTs) ? value.minTs : null;
        const maxTs = typeof value?.maxTs === 'number' && Number.isFinite(value.maxTs) ? value.maxTs : null;
        const messageCount =
          typeof value?.messageCount === 'number' && Number.isFinite(value.messageCount)
            ? value.messageCount
            : Array.isArray(value?.messages)
            ? value.messages.length
            : undefined;
        results.push({ id: cursor.primaryKey, size: value?.size || 0, minTs, maxTs, messageCount });
        cursor.continue();
      } else {
        resolve();
      }
    };
    request.onerror = () => reject(request.error);
  });
  await wrapTransaction(tx);
  const cutoff =
    options && typeof options.cutoff === 'number' && Number.isFinite(options.cutoff)
      ? options.cutoff
      : null;
  if (cutoff == null) {
    return results;
  }
  return results.filter((shard) => {
    if (shard.minTs == null && shard.maxTs == null) {
      return true;
    }
    if (shard.maxTs != null && shard.maxTs < cutoff) {
      return false;
    }
    return true;
  });
}

export async function getShardRecord(shardId) {
  if (shardId == null) return null;
  const db = await ensureDb();
  if (!db) return null;
  const tx = db.transaction(STORE_NAME, 'readonly');
  const store = tx.objectStore(STORE_NAME);
  const record = await wrapRequest(store.get(shardId));
  await wrapTransaction(tx);
  if (!record) return null;
  if ((record.minTs == null || record.maxTs == null) && Array.isArray(record.messages)) {
    let minTs = null;
    let maxTs = null;
    for (const msg of record.messages) {
      const ts = typeof msg?.ts === 'number' && Number.isFinite(msg.ts) ? msg.ts : null;
      if (ts == null) continue;
      minTs = minTs == null ? ts : Math.min(minTs, ts);
      maxTs = maxTs == null ? ts : Math.max(maxTs, ts);
    }
    if (minTs != null) record.minTs = minTs;
    if (maxTs != null) record.maxTs = maxTs;
  }
  if (record.messageCount == null && Array.isArray(record.messages)) {
    record.messageCount = record.messages.length;
  }
  return { id: shardId, ...record };
}

export async function readShardText(shardId) {
  const record = await getShardRecord(shardId);
  if (!record) return '';
  if (Array.isArray(record.messages)) {
    return record.messages.map((msg) => (msg && typeof msg.text === 'string' ? msg.text : '')).join('\n');
  }
  const blob = record.textBlob;
  if (!blob) return '';
  if (typeof blob.text === 'function') {
    try {
      return await blob.text();
    } catch (err) {
      console.error('Failed to read shard text', err);
      return '';
    }
  }
  try {
    return String(await blob);
  } catch (err) {
    console.error('Failed to coerce shard text', err);
    return '';
  }
}

export async function readShardMessages(shardId) {
  const record = await getShardRecord(shardId);
  if (!record) return [];
  if (Array.isArray(record.messages)) {
    return record.messages.map((msg) => ({
      text: msg && typeof msg.text === 'string' ? msg.text : '',
      ts: typeof msg?.ts === 'number' && Number.isFinite(msg.ts) ? msg.ts : null,
      model: typeof msg?.model === 'string' ? msg.model : null,
      role: typeof msg?.role === 'string' ? msg.role : null,
    }));
  }
  const text = await readShardText(shardId);
  if (!text) return [];
  const lines = text.split('\n');
  if (lines.length && lines[lines.length - 1] === '') {
    lines.pop();
  }
  return lines.map((line) => ({ text: line, ts: null, model: null }));
}

export async function clearAll() {
  if (!hasIndexedDB()) return;
  resetBuffer();
  persistedBytes = 0;
  lastShardId = null;
  const existingDb = await (dbPromise || Promise.resolve(null));
  existingDb?.close?.();
  dbPromise = null;
  initPromise = null;
  await new Promise((resolve, reject) => {
    const request = indexedDB.deleteDatabase(DB_NAME);
    request.onsuccess = () => resolve();
    request.onerror = () => reject(request.error);
    request.onblocked = () => resolve();
  });
}

export function getShardProgress() {
  return { persistedBytes, pendingBytes: bufferBytes };
}
