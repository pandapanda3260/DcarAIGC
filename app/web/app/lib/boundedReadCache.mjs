/** Process-local cache for optional read helpers. Failures are never cached. */
/** @template T */
export class BoundedReadCache {
  /** @type {Map<string, {value: T, expires: number, bytes: number}>} */
  values = new Map();
  /** @type {Map<string, Promise<T>>} */
  inflight = new Map();
  /** @type {Array<() => void>} */
  waiting = [];
  active = 0;
  bytes = 0;
  generation = 0;

  /** @param {{ttlMs: number, maxEntries: number, maxBytes: number, concurrency: number, maxQueued: number, queueTimeoutMs: number, clock?: () => number}} options */
  constructor(options) { this.options = options; }

  clear() {
    this.generation += 1;
    this.values.clear(); this.bytes = 0;
    // Existing callers finish, but new requests cannot join an obsolete load.
    this.inflight.clear();
  }

  now() { return this.options.clock?.() ?? performance.now(); }

  /** @returns {Promise<void>} */
  acquire() {
    if (this.active < this.options.concurrency) {
      this.active += 1;
      return Promise.resolve();
    }
    if (this.waiting.length >= this.options.maxQueued) return Promise.reject(new Error("Reader queue full"));
    return new Promise((resolve, reject) => {
      const grant = () => { clearTimeout(timer); this.active += 1; resolve(); };
      const timer = setTimeout(() => {
        this.waiting = this.waiting.filter((entry) => entry !== grant);
        reject(new Error("Reader queue timeout"));
      }, this.options.queueTimeoutMs);
      this.waiting.push(grant);
    });
  }

  release() {
    this.active -= 1;
    this.waiting.shift()?.();
  }

  /** @param {string} key @param {() => Promise<T>} loader @returns {Promise<T>} */
  get(key, loader) {
    const cached = this.values.get(key);
    if (cached) {
      if (cached.expires > this.now()) {
        this.values.delete(key); this.values.set(key, cached);
        return Promise.resolve(cached.value);
      }
      this.values.delete(key); this.bytes -= cached.bytes;
    }
    const existing = this.inflight.get(key);
    if (existing) return existing;
    const generation = this.generation;
    const result = (async () => {
      await this.acquire();
      try {
        const value = await loader();
        const bytes = Buffer.byteLength(JSON.stringify(value), "utf8");
        if (generation === this.generation && bytes <= this.options.maxBytes) {
          const previous = this.values.get(key);
          if (previous) this.bytes -= previous.bytes;
          this.values.set(key, { value, bytes, expires: this.now() + this.options.ttlMs });
          this.bytes += bytes;
          while (this.values.size > this.options.maxEntries || this.bytes > this.options.maxBytes) {
            const first = this.values.keys().next().value;
            this.bytes -= this.values.get(first).bytes;
            this.values.delete(first);
          }
        }
        return value;
      } finally { this.release(); }
    })();
    this.inflight.set(key, result);
    // Observe both branches so cache housekeeping cannot create an unhandled rejection.
    const finished = () => { if (this.inflight.get(key) === result) this.inflight.delete(key); };
    void result.then(finished, finished);
    return result;
  }
}
