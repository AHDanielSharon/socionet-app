import { db } from '@lib/db';
import { cache } from '@lib/redis';

export const searchService = {
  global: async (
    query: string,
    type: string = 'all',
    userId: string | null,
    limit: number = 20
  ) => {
    const sanitizedQuery = query.replace(/[%_\\]/g, '\\$&');
    const results: any = {};

    await Promise.all([
      // Users
      (type === 'all' || type === 'users') && db.queryMany(
        `SELECT u.id, u.username, u.full_name, u.avatar_url, u.is_verified,
                u.bio, u.followers_count, u.is_private,
                ts_rank(
                  to_tsvector('english', u.full_name || ' ' || u.username || ' ' || COALESCE(u.bio,'')),
                  plainto_tsquery('english', $1)
                ) AS rank,
                ${userId ? `EXISTS(SELECT 1 FROM relationships WHERE follower_id = $3 AND following_id = u.id AND type = 'follow' AND status = 'accepted') AS is_following` : 'false AS is_following'}
         FROM users u
         WHERE u.status = 'active'
           AND (
             u.username ILIKE $2
             OR u.full_name ILIKE $2
             OR to_tsvector('english', u.full_name || ' ' || u.username || ' ' || COALESCE(u.bio,''))
                @@ plainto_tsquery('english', $1)
           )
         ORDER BY rank DESC, u.followers_count DESC
         LIMIT $${userId ? 4 : 3}`,
        userId
          ? [query, `%${sanitizedQuery}%`, userId, Math.min(limit, 20)]
          : [query, `%${sanitizedQuery}%`, Math.min(limit, 20)]
      ).then(r => { results.users = r; }),

      // Posts
      (type === 'all' || type === 'posts') && db.queryMany(
        `SELECT p.id, p.type, p.caption, p.likes_count, p.comments_count, p.created_at,
                u.username, u.full_name, u.avatar_url, u.is_verified,
                (SELECT json_build_object('url', m.url, 'type', m.type, 'thumbnail_url', m.thumbnail_url,
                  'width', m.width, 'height', m.height, 'blurhash', m.blurhash)
                 FROM post_media pm JOIN media m ON m.id = pm.media_id
                 WHERE pm.post_id = p.id AND pm.position = 0 LIMIT 1) AS first_media,
                ts_rank(to_tsvector('english', COALESCE(p.caption,'')), plainto_tsquery('english', $1)) AS rank
         FROM posts p
         JOIN users u ON u.id = p.user_id
         WHERE p.visibility = 'public' AND p.type NOT IN ('story','live')
           AND u.status = 'active'
           AND to_tsvector('english', COALESCE(p.caption,'')) @@ plainto_tsquery('english', $1)
         ORDER BY rank DESC, p.likes_count DESC
         LIMIT $2`,
        [query, Math.min(limit, 20)]
      ).then(r => { results.posts = r; }),

      // Communities
      (type === 'all' || type === 'communities') && db.queryMany(
        `SELECT c.id, c.name, c.slug, c.description, c.avatar_url, c.members_count,
                c.posts_count, c.is_private, c.category,
                ${userId ? `EXISTS(SELECT 1 FROM community_members WHERE community_id = c.id AND user_id = $3 AND status = 'active') AS is_member` : 'false AS is_member'}
         FROM communities c
         WHERE c.status = 'active'
           AND (c.name ILIKE $2 OR c.description ILIKE $2)
         ORDER BY c.members_count DESC
         LIMIT $${userId ? 4 : 3}`,
        userId
          ? [query, `%${sanitizedQuery}%`, userId, 10]
          : [query, `%${sanitizedQuery}%`, 10]
      ).then(r => { results.communities = r; }),

      // Hashtags
      (type === 'all' || type === 'hashtags') && db.queryMany(
        `SELECT id, name, posts_count
         FROM hashtags WHERE name ILIKE $1
         ORDER BY posts_count DESC LIMIT 20`,
        [`${sanitizedQuery}%`]
      ).then(r => { results.hashtags = r; }),
    ].filter(Boolean));

    return results;
  },

  trending: async () => {
    return cache.getOrSet('search:trending', async () => {
      const topics = await db.queryMany(
        `SELECT h.name, h.posts_count,
                COUNT(ph.post_id) FILTER (WHERE p.created_at > NOW() - INTERVAL '24 hours') AS posts_today,
                COUNT(ph.post_id) FILTER (WHERE p.created_at > NOW() - INTERVAL '1 hour') AS posts_1h
         FROM hashtags h
         JOIN post_hashtags ph ON ph.hashtag_id = h.id
         JOIN posts p ON p.id = ph.post_id
         WHERE p.created_at > NOW() - INTERVAL '7 days'
         GROUP BY h.id
         HAVING COUNT(ph.post_id) > 0
         ORDER BY posts_today DESC, posts_1h DESC, h.posts_count DESC
         LIMIT 25`
      );
      return { trending: topics };
    }, 300);
  },

  saveHistory: async (userId: string, query: string): Promise<void> => {
    await db.query(
      `INSERT INTO search_history (user_id, query) VALUES ($1, $2)`,
      [userId, query.slice(0, 255)]
    ).catch(() => {}); // Best effort
  },

  getHistory: async (userId: string) => {
    return db.queryMany(
      `SELECT DISTINCT ON (query) query, MAX(created_at) AS last_searched
       FROM search_history WHERE user_id = $1
       GROUP BY query ORDER BY query, last_searched DESC
       LIMIT 20`,
      [userId]
    );
  },
};
