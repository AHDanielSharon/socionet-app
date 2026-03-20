import { db } from '@lib/db';
import { cache } from '@lib/redis';
import { logger } from '@utils/logger';
import crypto from 'crypto';

export const liveService = {
  startStream: async (userId: string, title: string, description?: string) => {
    const streamKey = crypto.randomBytes(16).toString('hex');
    const stream = await db.queryOne(
      `INSERT INTO live_streams (user_id, title, description, stream_key, status)
       VALUES ($1, $2, $3, $4, 'live') RETURNING *`,
      [userId, title, description || null, streamKey]
    );
    await cache.set(`live:stream:${userId}`, stream, 3600 * 8);
    return stream;
  },

  endStream: async (streamId: string, userId: string) => {
    const stream = await db.queryOne(
      `UPDATE live_streams SET status = 'ended', ended_at = NOW() WHERE id = $1 AND user_id = $2 RETURNING *`,
      [streamId, userId]
    );
    await cache.del(`live:stream:${userId}`);
    return stream;
  },

  getActiveStreams: async (limit = 20) => {
    return db.queryMany(
      `SELECT ls.*, u.username, u.full_name, u.avatar_url, u.is_verified,
              (SELECT COUNT(*) FROM live_viewers WHERE stream_id = ls.id) AS viewer_count
       FROM live_streams ls JOIN users u ON u.id = ls.user_id
       WHERE ls.status = 'live' ORDER BY ls.started_at DESC LIMIT $1`,
      [limit]
    );
  },

  recordView: async (streamId: string, userId: string) => {
    await db.query(
      `INSERT INTO live_viewers (stream_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
      [streamId, userId]
    );
  },
};
