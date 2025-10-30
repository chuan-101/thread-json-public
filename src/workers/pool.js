const DEFAULT_POOL_SIZE = 3;

export class WorkerPool {
  constructor(size, url) {
    if (!url) {
      throw new Error('WorkerPool requires a worker URL');
    }
    const poolSize = Number.isFinite(size) ? Math.max(1, Math.floor(size)) : DEFAULT_POOL_SIZE;
    this.size = Math.min(poolSize, 32); // hard cap to avoid runaway worker creation
    this.url = url;
    this._queue = [];
    this._workers = [];
    this._active = new Map();
    this._terminated = false;

    for (let i = 0; i < this.size; i += 1) {
      this._spawnWorker();
    }
  }

  run(payload) {
    if (this._terminated) {
      return Promise.reject(new Error('WorkerPool has been terminated'));
    }
    return new Promise((resolve, reject) => {
      this._queue.push({ payload, resolve, reject });
      this._drain();
    });
  }

  async terminate() {
    if (this._terminated) {
      return;
    }
    this._terminated = true;

    // Reject pending tasks
    while (this._queue.length) {
      const task = this._queue.shift();
      task.reject?.(new Error('WorkerPool terminated'));
    }

    const workers = this._workers.slice();
    this._workers.length = 0;
    this._active.clear();

    workers.forEach((worker) => {
      try {
        worker.terminate?.();
      } catch (err) {
        // ignore termination failures
      }
    });
  }

  _spawnWorker() {
    const worker = new Worker(this.url, { type: 'module' });
    const handleMessage = (event) => {
      const task = this._active.get(worker);
      if (!task) {
        return;
      }
      this._active.delete(worker);
      this._queueMicrotask(() => this._drain());
      task.resolve(event.data);
    };

    const handleError = (event) => {
      const task = this._active.get(worker);
      if (task) {
        this._active.delete(worker);
        task.reject(event instanceof ErrorEvent ? event.error || event.message : event);
      }
      this._workers = this._workers.filter((w) => w !== worker);
      try {
        worker.terminate?.();
      } catch (err) {
        // ignore errors terminating a failed worker
      }
      if (!this._terminated) {
        this._spawnWorker();
      }
      this._queueMicrotask(() => this._drain());
    };

    worker.addEventListener('message', handleMessage);
    worker.addEventListener('error', handleError);
    worker.addEventListener('messageerror', handleError);

    this._workers.push(worker);
    this._queueMicrotask(() => this._drain());
  }

  _takeIdleWorker() {
    for (let i = 0; i < this._workers.length; i += 1) {
      const worker = this._workers[i];
      if (!this._active.has(worker)) {
        return worker;
      }
    }
    return null;
  }

  _drain() {
    if (this._terminated) {
      return;
    }
    while (this._queue.length) {
      const worker = this._takeIdleWorker();
      if (!worker) {
        break;
      }
      const task = this._queue.shift();
      try {
        this._active.set(worker, task);
        worker.postMessage(task.payload);
      } catch (err) {
        this._active.delete(worker);
        task.reject(err);
        // Replace the worker if postMessage fails
        this._workers = this._workers.filter((w) => w !== worker);
        try {
          worker.terminate?.();
        } catch (terminateErr) {
          // ignore
        }
        if (!this._terminated) {
          this._spawnWorker();
        }
      }
    }
  }

  _queueMicrotask(cb) {
    if (typeof queueMicrotask === 'function') {
      queueMicrotask(cb);
    } else {
      Promise.resolve().then(cb);
    }
  }
}
