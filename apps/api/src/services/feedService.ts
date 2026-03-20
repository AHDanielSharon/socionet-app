import { db } from '@lib/db';
import { cache } from '@lib/redis';
import { logger } from '@utils/logger';

const FEED_CACHE_TTL = 60;
const FEED_LIMIT_DEFAULT = 20;

export const feedService = {
  // ── Personalized home feed
  getHomeFeed: async (
    userId: string,
    options: { limit?: number; cursor?: string } = {}
  ) => {
    const { limit = FEED_LIMIT_DEFAULT, cursor } = options;
    const cacheKey = `feed:home:${userId}:${cursor || 'first'}`;

    const cached = await cache.get<any>(cacheKey);
    if (cached) return cached;

    const result = await db.queryMany(
      `WITH
        following AS (
          SELECT following_id FROM relationships
          WHERE follower_id = $1 AND type = 'follow' AND status = 'accepted'
        ),
        my_communities AS (
          SELECT community_id FROM community_members
          WHERE user_id = $1 AND status = 'active'
        ),
        candidates AS (
          -- Own posts
          SELECT p.id, 1.0 AS weight, 'own' AS source, p.created_at
          FROM posts p
          WHERE p.user_id = $1 AND p.type NOT IN ('story') AND p.archived_at IS NULL
          ${cursor ? `AND p.created_at < '${cursor}'` : ''}
          UNION ALL
          -- Following posts
          SELECT p.id, 0.9 AS weight, 'following' AS source, p.created_at
          FROM posts p
          WHERE p.user_id IN (SELECT following_id FROM following)
            AND p.visibility IN ('public','followers')
            AND p.type NOT IN ('story') AND p.archived_at IS NULL
          ${cursor ? `AND p.created_at < '${cursor}'` : ''}
          UNION ALL
          -- Community posts
          SELECT p.id, 0.8 AS weight, 'community' AS source, p.created_at
          FROM community_posts cp
          JOIN posts p ON p.id = cp.post_id
          WHERE cp.community_id IN (SELECT community_id FROM my_communities)
            AND p.type NOT IN ('story') AND p.archived_at IS NULL
          ${cursor ? `AND p.created_at < '${cursor}'` : ''}
          UNION ALL
          -- Trending popular posts (injected discovery)
          SELECT p.id, 0.3 AS weight, 'discover' AS source, p.created_at
          FROM posts p
          WHERE p.visibility = 'public'
            AND p.type NOT IN ('story','live')
            AND p.user_id NOT IN (SELECT following_id FROM following)
            AND p.user_id != $1
            AND p.created_at > NOW() - INTERVAL '72 hours'
            AND p.likes_count > 5
            AND p.archived_at IS NULL
          ${cursor ? `AND p.created_at < '${cursor}'` : ''}
          ORDER BY p.likes_count DESC
          LIMIT 3
        ),
        deduped AS (
          SELECT DISTINCT ON (id) id, weight, source
          FROM candidates
          ORDER BY id, weight DESC
        )
        SELECT
          p.*,
          u.username, u.full_name, u.avatar_url, u.is_verified, u.is_creator,
          d.weight, d.source,
          -- Viewer-specific
          EXISTS(SELECT 1 FROM post_reactions pr WHERE pr.post_id = p.id AND pr.user_id = $1) AS is_liked,
          EXISTS(SELECT 1 FROM post_saves ps WHERE ps.post_id = p.id AND ps.user_id = $1) AS is_saved,
          (SELECT reaction::text FROM post_reactions WHERE post_id = p.id AND user_id = $1 LIMIT 1) AS my_reaction,
          -- Media
          (
            SELECT COALESCE(json_agg(json_build_object(
              'id', m.id, 'type', m.type, 'url', m.url,
              'thumbnail_url', m.thumbnail_url, 'width', m.width,
              'height', m.height, 'duration_seconds', m.duration_seconds,
              'blurhash', m.blurhash, 'position', pm.position, 'alt_text', pm.alt_text
            ) ORDER BY pm.position), '[]')
            FROM post_media pm JOIN media m ON m.id = pm.media_id WHERE pm.post_id = p.id
          ) AS media,
          -- Quoted post
          CASE WHEN p.quoted_post_id IS NOT NULL THEN (
            SELECT json_build_object(
              'id', qp.id, 'caption', qp.caption,
              'user_username', qu.username, 'user_full_name', qu.full_name,
              'user_avatar_url', qu.avatar_url
            )
            FROM posts qp JOIN users qu ON qu.id = qp.user_id
            WHERE qp.id = p.quoted_post_id
          ) END AS quoted_post
        FROM deduped d
        JOIN posts p ON p.id = d.id
        JOIN users u ON u.id = p.user_id
        WHERE u.status = 'active'
          AND u.id NOT IN (
            SELECT following_id FROM relationships WHERE follower_id = $1 AND type = 'blocked'
          )
        ORDER BY
          (
            EXTRACT(EPOCH FROM p.created_at) / 3600.0
            + d.weight * LN(GREATEST(p.likes_count * 2 + p.comments_count * 3 + p.shares_count + p.views_count * 0.1, 1))
          ) DESC
        LIMIT $2`,
      [userId, limit + 1]
    );

    const hasMore = result.length > limit;
    const posts = result.slice(0, limit);
    const nextCursor = hasMore ? posts[posts.length - 1].created_at : null;

    const response = { posts, next_cursor: nextCursor, has_more: hasMore };
    await cache.set(cacheKey, response, FEED_CACHE_TTL);
    return response;
  },

  // ── Reels feed (short videos)
  getReelsFeed: async (userId: string, options: { limit?: number; cursor?: string } = {}) => {
    const { limit = 10, cursor } = options;

    const result = await db.queryMany(
      `SELECT p.*, u.username, u.full_name, u.avatar_url, u.is_verified,
              m.url AS video_url, m.thumbnail_url, m.duration_seconds, m.width, m.height, m.blurhash,
              EXISTS(SELECT 1 FROM post_reactions WHERE post_id = p.id AND user_id = $1) AS is_liked,
              EXISTS(SELECT 1 FROM relationships WHERE follower_id = $1 AND following_id = p.user_id AND type = 'follow' AND status = 'accepted') AS is_following
       FROM posts p
       JOIN users u ON u.id = p.user_id
       JOIN post_media pm ON pm.post_id = p.id AND pm.position = 0
       JOIN media m ON m.id = pm.media_id AND m.type = 'video'
       WHERE p.type = 'reel' AND p.visibility = 'public'
         AND u.status = 'active'
         AND u.id NOT IN (
           SELECT following_id FROM relationships WHERE follower_id = $1 AND type = 'blocked'
         )
         ${cursor ? `AND p.created_at < '${cursor}'` : ''}
       ORDER BY
         (p.likes_count * 3 + p.comments_count * 4 + p.views_count * 0.2 + p.shares_count * 2)
         * EXP(-0.05 * EXTRACT(EPOCH FROM (NOW() - p.created_at)) / 3600) DESC
       LIMIT $2`,
      [userId, limit]
    );

    return { reels: result };
  },

  // ── Stories feed
  getStoriesFeed: async (userId: string) => {
    const [ownStories, followingStories] = await Promise.all([
      // Own stories
      db.queryMany(
        `SELECT p.id, p.created_at, p.expires_at,
                (SELECT json_build_object('url', m.url, 'type', m.type, 'width', m.width,
                  'height', m.height, 'thumbnail_url', m.thumbnail_url, 'blurhash', m.blurhash)
                 FROM post_media pm JOIN media m ON m.id = pm.media_id WHERE pm.post_id = p.id LIMIT 1) AS media,
                (SELECT COUNT(*) FROM story_views WHERE story_id = p.id) AS views_count
         FROM posts p
         WHERE p.user_id = $1 AND p.type = 'story' AND p.expires_at > NOW()
         ORDER BY p.created_at DESC`,
        [userId]
      ),

      // Following stories
      db.queryMany(
        `SELECT
           u.id AS user_id, u.username, u.full_name, u.avatar_url, u.is_verified,
           COUNT(p.id) AS stories_count,
           MAX(p.created_at) AS latest_story_at,
           BOOL_OR(sv.viewer_id IS NULL) AS has_unseen,
           json_agg(json_build_object(
             'id', p.id,
             'created_at', p.created_at,
             'expires_at', p.expires_at,
             'seen', sv.viewer_id IS NOT NULL,
             'caption', p.caption,
             'media', (
               SELECT json_build_object('url', m.url, 'type', m.type, 'width', m.width,
                 'height', m.height, 'thumbnail_url', m.thumbnail_url, 'blurhash', m.blurhash)
               FROM post_media pm2 JOIN media m ON m.id = pm2.media_id WHERE pm2.post_id = p.id LIMIT 1
             )
           ) ORDER BY p.created_at ASC) AS stories
         FROM relationships r
         JOIN users u ON u.id = r.following_id
         JOIN posts p ON p.user_id = u.id
           AND p.type = 'story' AND p.expires_at > NOW()
           AND p.visibility NOT IN ('only_me')
         LEFT JOIN story_views sv ON sv.story_id = p.id AND sv.viewer_id = $1
         WHERE r.follower_id = $1 AND r.type = 'follow' AND r.status = 'accepted'
           AND u.status = 'active'
         GROUP BY u.id
         ORDER BY has_unseen DESC, latest_story_at DESC`,
        [userId]
      ),
    ]);

    return { own_stories: ownStories, following_stories: followingStories };
  },

  // ── Video feed (long-form)
  getVideoFeed: async (userId: string | null, options: { limit?: number; offset?: number } = {}) => {
    const { limit = 20, offset = 0 } = options;

    const result = await db.queryMany(
      `SELECT p.*, u.username, u.full_name, u.avatar_url, u.is_verified,
              m.url AS video_url, m.thumbnail_url, m.duration_seconds,
              ${userId ? `EXISTS(SELECT 1 FROM post_reactions WHERE post_id = p.id AND user_id = $3) AS is_liked,
              EXISTS(SELECT 1 FROM relationships WHERE follower_id = $3 AND following_id = p.user_id AND type = 'follow') AS is_following` :
              'false AS is_liked, false AS is_following'}
       FROM posts p
       JOIN users u ON u.id = p.user_id
       JOIN post_media pm ON pm.post_id = p.id AND pm.position = 0
       JOIN media m ON m.id = pm.media_id AND m.type = 'video'
       WHERE p.type IN ('video','podcast') AND p.visibility = 'public'
         AND u.status = 'active'
       ORDER BY p.views_count DESC, p.created_at DESC
       LIMIT $1 OFFSET $2`,
      userId ? [limit, offset, userId] : [limit, offset]
    );

    return { videos: result };
  },

  // ── Invalidate a user's feed cache
  invalidate: async (userId: string): Promise<void> => {
    await cache.invalidatePattern(`feed:home:${userId}:*`);
  },

  // ── Invalidate feeds of all followers (on new post)
  invalidateFollowerFeeds: async (userId: string): Promise<void> => {
    try {
      const followers = await db.queryMany<{ follower_id: string }>(
        `SELECT follower_id FROM relationships
         WHERE following_id = $1 AND type = 'follow' AND status = 'accepted'
         LIMIT 1000`,
        [userId]
      );
      await Promise.all(followers.map(f => feedService.invalidate(f.follower_id)));
    } catch (err) {
      logger.error('Failed to invalidate follower feeds', { error: String(err) });
    }
  },
};
