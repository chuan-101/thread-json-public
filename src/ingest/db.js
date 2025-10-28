const DB_NAME = 'echo-json-shards';
const DB_VERSION = 1;
const STORE_NAME = 'shards';
const MIN_SHARD_BYTES = 12 * 1024 * 1024; // 12MB
const MAX_SHARD_BYTES = 16 * 1024 * 1024; // 16MB target upper bound

let dbPromise = null;
let initPromise = null;
let bufferBlob = null;
let bufferBytes = 0;
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
  bufferBlob = null;
  bufferBytes = 0;
}

async function flushBuffer(db, force = false) {
  if (!bufferBytes) return lastShardId;
  if (!force && bufferBytes < MIN_SHARD_BYTES) return lastShardId;
  if (!db) {
    resetBuffer();
    return lastShardId;
  }

  let flushedId = lastShardId;
  while (bufferBytes && (force || bufferBytes >= MIN_SHARD_BYTES)) {
    const takeBytes = force ? bufferBytes : Math.min(bufferBytes, MAX_SHARD_BYTES);
    const shardBlob = bufferBlob.slice(0, takeBytes, 'text/plain');
    const tx = db.transaction(STORE_NAME, 'readwrite');
    const store = tx.objectStore(STORE_NAME);
    const id = await wrapRequest(store.add({ textBlob: shardBlob, size: shardBlob.size }));
    await wrapTransaction(tx);
    persistedBytes += shardBlob.size;
    lastShardId = id;
    flushedId = id;

    if (shardBlob.size >= bufferBytes) {
      resetBuffer();
    } else {
      bufferBlob = bufferBlob.slice(shardBlob.size, bufferBytes, 'text/plain');
      bufferBytes = bufferBlob.size;
    }

    if (!force && bufferBytes < MIN_SHARD_BYTES) {
      break;
    }
  }

  return flushedId;
}

export async function openDB() {
  return ensureDb();
}

export async function appendToShard(textSlice) {
  if (!textSlice) return lastShardId;
  const db = await ensureDb();
  if (!db) return lastShardId;
  const sliceBlob = new Blob([textSlice], { type: 'text/plain' });
  bufferBlob = bufferBlob ? new Blob([bufferBlob, sliceBlob], { type: 'text/plain' }) : sliceBlob;
  bufferBytes = bufferBlob.size;
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

export async function listShards() {
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
        results.push({ id: cursor.primaryKey, size: value?.size || 0 });
        cursor.continue();
      } else {
        resolve();
      }
    };
    request.onerror = () => reject(request.error);
  });
  await wrapTransaction(tx);
  return results;
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
