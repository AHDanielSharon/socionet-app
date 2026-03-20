import { Router, Response } from 'express';
import { db } from '@lib/db';
import { cache } from '@lib/redis';
import { authenticate, optionalAuth, AuthRequest } from '@middleware/auth';
import { uploadProfileMedia } from '@middleware/upload';
import { validate } from '@middleware/validate';
import { updateProfileSchema, updateUsernameSchema } from '@validators/schemas';
import { uploadAvatar, uploadBanner } from '@lib/storage';
import { AppError, NotFoundError, ConflictError, ForbiddenError } from '@middleware/errorHandler';
import { clearUserCache } from '@middleware/auth';

const router = Router();

router.get('/me/suggestions', authenticate, async (req: AuthRequest, res: Response) => {
  const result = await db.queryMany(
    `SELECT DISTINCT u.id, u.username, u.full_name, u.avatar_url, u.is_verified, u.bio, u.followers_count,
            (SELECT COUNT(*) FROM relationships r1 JOIN relationships r2 ON r2.following_id = r1.following_id WHERE r1.follower_id = $1 AND r2.follower_id = u.id AND r1.type = 'follow' AND r2.type = 'follow') AS mutual_count
     FROM users u
     WHERE u.id != $1 AND u.status = 'active' AND u.deleted_at IS NULL
       AND u.id NOT IN (SELECT following_id FROM relationships WHERE follower_id = $1 AND type IN ('follow','blocked'))
     ORDER BY mutual_count DESC, u.followers_count DESC LIMIT 20`,
    [req.user!.id]
  );
  res.json({ users: result });
});

router.get('/:username', optionalAuth, async (req: AuthRequest, res: Response) => {
  const { username } = req.params;
  const uid = req.user?.id;
  const user = await db.queryOne(
    `SELECT u.id, u.username, u.full_name, u.display_name, u.bio, u.website, u.avatar_url, u.banner_url,
            u.location, u.is_verified, u.is_private, u.is_online, u.last_seen_at, u.is_creator, u.is_business,
            u.followers_count, u.following_count, u.posts_count, u.created_at,
            ${uid ? `
            EXISTS(SELECT 1 FROM relationships WHERE follower_id = $2 AND following_id = u.id AND type = 'follow' AND status = 'accepted') AS is_following,
            EXISTS(SELECT 1 FROM relationships WHERE follower_id = u.id AND following_id = $2 AND type = 'follow' AND status = 'accepted') AS follows_you,
            EXISTS(SELECT 1 FROM relationships WHERE follower_id = $2 AND following_id = u.id AND type = 'blocked') AS is_blocked,
            EXISTS(SELECT 1 FROM relationships WHERE follower_id = $2 AND following_id = u.id AND type = 'follow' AND status = 'pending') AS follow_requested
            ` : 'false AS is_following, false AS follows_you, false AS is_blocked, false AS follow_requested'}
     FROM users u
     WHERE LOWER(u.username) = LOWER($1) AND u.status = 'active' AND u.deleted_at IS NULL`,
    uid ? [username, uid] : [username]
  );
  if (!user) throw new NotFoundError('User');
  if (user.is_blocked) throw new ForbiddenError('This user has blocked you');
  res.json({ user });
});

router.get('/:username/posts', optionalAuth, async (req: AuthRequest, res: Response) => {
  const { username } = req.params;
  const { limit = '18', cursor, type } = req.query;
  const uid = req.user?.id;
  const target = await db.queryOne('SELECT id, is_private FROM users WHERE LOWER(username) = LOWER($1) AND status = $2', [username, 'active']);
  if (!target) throw new NotFoundError('User');
  if (target.is_private && uid !== target.id) {
    const follows = await db.queryOne('SELECT 1 FROM relationships WHERE follower_id = $1 AND following_id = $2 AND type = $3 AND status = $4', [uid, target.id, 'follow', 'accepted']);
    if (!follows) return res.status(403).json({ error: 'Account is private', code: 'PRIVATE' });
  }
  const posts = await db.queryMany(
    `SELECT p.id, p.type, p.caption, p.likes_count, p.comments_count, p.views_count, p.created_at, p.is_pinned,
            (SELECT json_agg(json_build_object('url', m.url, 'type', m.type, 'thumbnail_url', m.thumbnail_url, 'width', m.width, 'height', m.height, 'blurhash', m.blurhash) ORDER BY pm.position)
             FROM post_media pm JOIN media m ON m.id = pm.media_id WHERE pm.post_id = p.id) AS media,
            ${uid ? `EXISTS(SELECT 1 FROM post_reactions WHERE post_id = p.id AND user_id = $4) AS is_liked` : 'false AS is_liked'}
     FROM posts p
     WHERE p.user_id = $1 ${type ? `AND p.type = '${type}'` : "AND p.type NOT IN ('story')"}
       ${cursor ? `AND p.created_at < '${cursor}'` : ''}
       AND p.archived_at IS NULL
     ORDER BY p.is_pinned DESC, p.created_at DESC LIMIT $2 OFFSET $3`,
    uid ? [target.id, limit, 0, uid] : [target.id, limit, 0]
  );
  res.json({ posts, has_more: posts.length === parseInt(limit as string) });
});

router.get('/:username/followers', optionalAuth, async (req: AuthRequest, res: Response) => {
  const target = await db.queryOne('SELECT id FROM users WHERE LOWER(username) = LOWER($1)', [req.params.username]);
  if (!target) throw new NotFoundError('User');
  const { limit = '30', offset = '0' } = req.query;
  const uid = req.user?.id;
  const users = await db.queryMany(
    `SELECT u.id, u.username, u.full_name, u.avatar_url, u.is_verified, u.bio, u.followers_count,
            ${uid ? `EXISTS(SELECT 1 FROM relationships WHERE follower_id = $4 AND following_id = u.id AND type = 'follow' AND status = 'accepted') AS is_following` : 'false AS is_following'}
     FROM relationships r JOIN users u ON u.id = r.follower_id
     WHERE r.following_id = $1 AND r.type = 'follow' AND r.status = 'accepted' AND u.status = 'active'
     ORDER BY r.created_at DESC LIMIT $2 OFFSET $3`,
    uid ? [target.id, limit, offset, uid] : [target.id, limit, offset]
  );
  res.json({ users });
});

router.get('/:username/following', optionalAuth, async (req: AuthRequest, res: Response) => {
  const target = await db.queryOne('SELECT id FROM users WHERE LOWER(username) = LOWER($1)', [req.params.username]);
  if (!target) throw new NotFoundError('User');
  const { limit = '30', offset = '0' } = req.query;
  const uid = req.user?.id;
  const users = await db.queryMany(
    `SELECT u.id, u.username, u.full_name, u.avatar_url, u.is_verified, u.bio, u.followers_count,
            ${uid ? `EXISTS(SELECT 1 FROM relationships WHERE follower_id = $4 AND following_id = u.id AND type = 'follow' AND status = 'accepted') AS is_following` : 'false AS is_following'}
     FROM relationships r JOIN users u ON u.id = r.following_id
     WHERE r.follower_id = $1 AND r.type = 'follow' AND r.status = 'accepted' AND u.status = 'active'
     ORDER BY r.created_at DESC LIMIT $2 OFFSET $3`,
    uid ? [target.id, limit, offset, uid] : [target.id, limit, offset]
  );
  res.json({ users });
});

router.patch('/me/profile', authenticate, uploadProfileMedia, validate(updateProfileSchema), async (req: AuthRequest, res: Response) => {
  const files = req.files as { [k: string]: Express.Multer.File[] };
  const { full_name, display_name, bio, website, location, date_of_birth, gender, pronouns, is_private, allow_messages_from, allow_tags_from, show_activity_status, show_read_receipts } = req.body;

  let avatar_url: string | undefined;
  let banner_url: string | undefined;

  if (files?.avatar?.[0]) {
    const r = await uploadAvatar(files.avatar[0].buffer);
    avatar_url = r.url;
  }
  if (files?.banner?.[0]) {
    const r = await uploadBanner(files.banner[0].buffer);
    banner_url = r.url;
  }

  const user = await db.queryOne(
    `UPDATE users SET
       full_name = COALESCE($1, full_name), display_name = COALESCE($2, display_name),
       bio = COALESCE($3, bio), website = COALESCE($4, website), location = COALESCE($5, location),
       date_of_birth = COALESCE($6::date, date_of_birth), gender = COALESCE($7::user_gender, gender),
       pronouns = COALESCE($8, pronouns), is_private = COALESCE($9::boolean, is_private),
       allow_messages_from = COALESCE($10, allow_messages_from), allow_tags_from = COALESCE($11, allow_tags_from),
       show_activity_status = COALESCE($12::boolean, show_activity_status), show_read_receipts = COALESCE($13::boolean, show_read_receipts),
       avatar_url = COALESCE($14, avatar_url), banner_url = COALESCE($15, banner_url),
       updated_at = NOW()
     WHERE id = $16
     RETURNING id, username, full_name, display_name, bio, website, avatar_url, banner_url, location, is_verified, is_private, followers_count, following_count, posts_count`,
    [full_name, display_name, bio, website, location, date_of_birth || null, gender || null, pronouns || null,
     is_private !== undefined ? is_private === 'true' || is_private === true : null,
     allow_messages_from, allow_tags_from,
     show_activity_status !== undefined ? show_activity_status === 'true' || show_activity_status === true : null,
     show_read_receipts !== undefined ? show_read_receipts === 'true' || show_read_receipts === true : null,
     avatar_url || null, banner_url || null, req.user!.id]
  );

  await clearUserCache(req.user!.id);
  res.json({ user });
});

router.patch('/me/username', authenticate, validate(updateUsernameSchema), async (req: AuthRequest, res: Response) => {
  const { username } = req.body;
  const exists = await db.queryOne('SELECT id FROM users WHERE LOWER(username) = $1 AND id != $2', [username, req.user!.id]);
  if (exists) throw new ConflictError('Username already taken');
  await db.query('UPDATE users SET username = $1, updated_at = NOW() WHERE id = $2', [username, req.user!.id]);
  await clearUserCache(req.user!.id);
  res.json({ username });
});

router.delete('/me/account', authenticate, async (req: AuthRequest, res: Response) => {
  await db.query('UPDATE users SET deleted_at = NOW(), status = $1, email = NULL, phone = NULL WHERE id = $2', ['deactivated', req.user!.id]);
  await clearUserCache(req.user!.id);
  res.json({ deleted: true });
});

export default router;
