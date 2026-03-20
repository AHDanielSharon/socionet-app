// Browser cache utilities
const CACHE_PREFIX = 'socionet:';
const DEFAULT_TTL = 5 * 60 * 1000;
const store = new Map<string, { data: any; expires: number }>();
export const memCache = {
  get: <T>(key: string): T | null => { const item = store.get(CACHE_PREFIX + key); if (!item || Date.now() > item.expires) { store.delete(CACHE_PREFIX + key); return null; } return item.data; },
  set: (key: string, data: any, ttl = DEFAULT_TTL) => store.set(CACHE_PREFIX + key, { data, expires: Date.now() + ttl }),
  delete: (key: string) => store.delete(CACHE_PREFIX + key),
  clear: () => store.clear(),
};
