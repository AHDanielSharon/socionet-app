import { createClient, RedisClientType } from 'redis';
import { config } from '@config/index';
import { logger } from '@utils/logger';

const PREFIX = config.redis.prefix;

let redisClient: RedisClientType;

const createRedisClient = () => {
  const client = createClient({
    url: config.redis.url,
    socket: {
      reconnectStrategy: (retries) => {
        if (retries > 20) return new Error('Max reconnection attempts reached');
        return Math.min(retries * 100, 5000);
      },
      connectTimeout: 5000,
    },
    pingInterval: 30000,
  }) as RedisClientType;

  client.on('error', (err) => logger.error('Redis error', { error: err.message }));
  client.on('connect', () => logger.info('Redis connected'));
  client.on('reconnecting', () => logger.warn('Redis reconnecting...'));
  client.on('ready', () => logger.info('Redis ready'));

  return client;
};

export const initRedis = async () => {
  redisClient = createRedisClient();
  await redisClient.connect();
  return redisClient;
};

export const getRedis = (): RedisClientType => {
  if (!redisClient) throw new Error('Redis not initialized');
  return redisClient;
};

const k = (key: string) => `${PREFIX}${key}`;

// ── Cache helpers
export const cache = {
  get: async <T>(key: string): Promise<T | null> => {
    try {
      const val = await redisClient.get(k(key));
      if (!val) return null;
      return JSON.parse(val) as T;
    } catch { return null; }
  },

  set: async (key: string, value: any, ttlSeconds?: number): Promise<void> => {
    const serialized = JSON.stringify(value);
    if (ttlSeconds) {
      await redisClient.setEx(k(key), ttlSeconds, serialized);
    } else {
      await redisClient.set(k(key), serialized);
    }
  },

  del: async (key: string | string[]): Promise<void> => {
    const keys = Array.isArray(key) ? key.map(k) : [k(key)];
    if (keys.length > 0) await redisClient.del(keys);
  },

  exists: async (key: string): Promise<boolean> => {
    return (await redisClient.exists(k(key))) > 0;
  },

  expire: async (key: string, ttlSeconds: number): Promise<void> => {
    await redisClient.expire(k(key), ttlSeconds);
  },

  incr: async (key: string): Promise<number> => {
    return redisClient.incr(k(key));
  },

  incrEx: async (key: string, ttlSeconds: number): Promise<number> => {
    const val = await redisClient.incr(k(key));
    if (val === 1) await redisClient.expire(k(key), ttlSeconds);
    return val;
  },

  invalidatePattern: async (pattern: string): Promise<void> => {
    const keys = await redisClient.keys(`${PREFIX}${pattern}`);
    if (keys.length > 0) await redisClient.del(keys);
  },

  getOrSet: async <T>(
    key: string,
    fn: () => Promise<T>,
    ttlSeconds: number
  ): Promise<T> => {
    const cached = await cache.get<T>(key);
    if (cached !== null) return cached;
    const fresh = await fn();
    await cache.set(key, fresh, ttlSeconds);
    return fresh;
  },

  // Hash operations
  hget: async <T>(key: string, field: string): Promise<T | null> => {
    const val = await redisClient.hGet(k(key), field);
    if (!val) return null;
    try { return JSON.parse(val); } catch { return val as any; }
  },

  hset: async (key: string, field: string, value: any): Promise<void> => {
    await redisClient.hSet(k(key), field, JSON.stringify(value));
  },

  hgetall: async <T>(key: string): Promise<Record<string, T>> => {
    const all = await redisClient.hGetAll(k(key));
    const result: Record<string, T> = {};
    for (const [field, val] of Object.entries(all)) {
      try { result[field] = JSON.parse(val); } catch { result[field] = val as any; }
    }
    return result;
  },

  // List operations
  lpush: async (key: string, ...values: any[]): Promise<void> => {
    await redisClient.lPush(k(key), values.map(v => JSON.stringify(v)));
  },

  lrange: async <T>(key: string, start: number, stop: number): Promise<T[]> => {
    const vals = await redisClient.lRange(k(key), start, stop);
    return vals.map(v => { try { return JSON.parse(v); } catch { return v as any; } });
  },

  // Set operations
  sadd: async (key: string, ...members: string[]): Promise<void> => {
    await redisClient.sAdd(k(key), members);
  },

  sismember: async (key: string, member: string): Promise<boolean> => {
    return redisClient.sIsMember(k(key), member);
  },

  smembers: async (key: string): Promise<string[]> => {
    return redisClient.sMembers(k(key));
  },

  srem: async (key: string, ...members: string[]): Promise<void> => {
    await redisClient.sRem(k(key), members);
  },
};

// ── Presence tracking
export const presence = {
  TTL: 300, // 5 minutes

  setOnline: async (userId: string): Promise<void> => {
    await redisClient.setEx(k(`presence:${userId}`), presence.TTL, 'online');
    await publish('presence', { userId, status: 'online', timestamp: Date.now() });
  },

  setOffline: async (userId: string): Promise<void> => {
    await redisClient.del(k(`presence:${userId}`));
    await publish('presence', { userId, status: 'offline', timestamp: Date.now() });
  },

  isOnline: async (userId: string): Promise<boolean> => {
    return (await redisClient.exists(k(`presence:${userId}`))) === 1;
  },

  refresh: async (userId: string): Promise<void> => {
    await redisClient.expire(k(`presence:${userId}`), presence.TTL);
  },

  getOnlineUsersFromList: async (userIds: string[]): Promise<string[]> => {
    if (!userIds.length) return [];
    const pipeline = redisClient.multi();
    userIds.forEach(id => pipeline.exists(k(`presence:${id}`)));
    const results = await pipeline.exec();
    return userIds.filter((_, i) => results[i] === 1);
  },
};

// ── Typing indicators
export const typing = {
  TTL: 8,

  start: async (conversationId: string, userId: string): Promise<void> => {
    await redisClient.setEx(k(`typing:${conversationId}:${userId}`), typing.TTL, '1');
    await publish('typing', { conversationId, userId, isTyping: true });
  },

  stop: async (conversationId: string, userId: string): Promise<void> => {
    await redisClient.del(k(`typing:${conversationId}:${userId}`));
    await publish('typing', { conversationId, userId, isTyping: false });
  },

  getTypers: async (conversationId: string): Promise<string[]> => {
    const keys = await redisClient.keys(`${PREFIX}typing:${conversationId}:*`);
    return keys.map(key => key.replace(`${PREFIX}typing:${conversationId}:`, ''));
  },
};

// ── Token blacklist
export const tokenBlacklist = {
  add: async (jti: string, expiresIn: number): Promise<void> => {
    await redisClient.setEx(k(`blacklist:${jti}`), expiresIn, '1');
  },
  isBlacklisted: async (jti: string): Promise<boolean> => {
    return (await redisClient.exists(k(`blacklist:${jti}`))) === 1;
  },
};

// ── Rate limiting helpers
export const rateLimit = {
  check: async (key: string, limit: number, windowSec: number): Promise<{ allowed: boolean; count: number }> => {
    const count = await cache.incrEx(`ratelimit:${key}`, windowSec);
    return { allowed: count <= limit, count };
  },
};

// ── Pub/Sub
export const publish = async (channel: string, data: any): Promise<void> => {
  try {
    await redisClient.publish(`${PREFIX}${channel}`, JSON.stringify(data));
  } catch (err) {
    logger.error('Redis publish error', { channel, error: String(err) });
  }
};

export const subscribe = async (
  channel: string,
  handler: (data: any) => void | Promise<void>
): Promise<void> => {
  const subscriber = createRedisClient();
  await subscriber.connect();
  await subscriber.subscribe(`${PREFIX}${channel}`, async (message) => {
    try {
      const data = JSON.parse(message);
      await handler(data);
    } catch (err) {
      logger.error('Redis subscribe handler error', { channel, error: String(err) });
    }
  });
};

export const disconnectRedis = async () => {
  if (redisClient) await redisClient.quit();
};
