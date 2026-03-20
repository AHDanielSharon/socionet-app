import { db } from '@lib/db';
import { cache } from '@lib/redis';
import { slugify } from '@utils/helpers';

export const communityService = {
  create: async (userId: string, data: any) => {
    const slug = slugify(data.name) + '-' + Math.random().toString(36).slice(2, 6);
    return db.transaction(async (client) => {
      const c = await client.query(
        `INSERT INTO communities (name, slug, description, category, tags, is_private, requires_approval, is_nsfw, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
        [data.name, slug, data.description, data.category, JSON.stringify(data.tags || []), data.is_private || false, data.requires_approval || false, data.is_nsfw || false, userId]
      );
      await client.query(
        `INSERT INTO community_members (community_id, user_id, role, status) VALUES ($1, $2, 'owner', 'active')`,
        [c.rows[0].id, userId]
      );
      return c.rows[0];
    });
  },

  getBySlug: async (slug: string, viewerId?: string) => {
    return db.queryOne(
      `SELECT c.*,
              ${viewerId ? `EXISTS(SELECT 1 FROM community_members WHERE community_id = c.id AND user_id = $2 AND status = 'active') AS is_member,
              (SELECT role FROM community_members WHERE community_id = c.id AND user_id = $2) AS my_role` : 'false AS is_member, null AS my_role'}
       FROM communities c WHERE c.slug = $1 AND c.status = 'active'`,
      viewerId ? [slug, viewerId] : [slug]
    );
  },

  join: async (communityId: string, userId: string) => {
    const community = await db.queryOne('SELECT requires_approval FROM communities WHERE id = $1', [communityId]);
    const status = community?.requires_approval ? 'pending' : 'active';
    return db.query(
      `INSERT INTO community_members (community_id, user_id, role, status) VALUES ($1, $2, 'member', $3) ON CONFLICT (community_id, user_id) DO UPDATE SET status = $3`,
      [communityId, userId, status]
    );
  },

  leave: async (communityId: string, userId: string) => {
    return db.query(
      `DELETE FROM community_members WHERE community_id = $1 AND user_id = $2`,
      [communityId, userId]
    );
  },
};
