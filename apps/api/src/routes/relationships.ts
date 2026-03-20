import { Router, Response } from 'express';
import { db } from '@lib/db';
import { cache } from '@lib/redis';
import { authenticate, AuthRequest } from '@middleware/auth';
import { AppError, NotFoundError } from '@middleware/errorHandler';
import { notificationService } from '@services/notificationService';
import { feedService } from '@services/feedService';

const router = Router();

router.post('/follow/:userId', authenticate, async (req: AuthRequest, res: Response) => {
  const { userId } = req.params;
  const followerId = req.user!.id;
  if (userId === followerId) throw new AppError('Cannot follow yourself', 400);
  const blocked = await db.queryOne('SELECT 1 FROM relationships WHERE follower_id = $1 AND following_id = $2 AND type = $3', [userId, followerId, 'blocked']);
  if (blocked) throw new AppError('Unable to follow this user', 403);
  const target = await db.queryOne('SELECT is_private FROM users WHERE id = $1', [userId]);
  if (!target) throw new NotFoundError('User');
  const status = target.is_private ? 'pending' : 'accepted';
  const existing = await db.queryOne('SELECT id, status FROM relationships WHERE follower_id = $1 AND following_id = $2 AND type = $3', [followerId, userId, 'follow']);
  if (existing) return res.status(409).json({ error: existing.status === 'pending' ? 'Follow request already sent' : 'Already following', status: existing.status });
  await db.query('INSERT INTO relationships (follower_id, following_id, type, status) VALUES ($1, $2, $3, $4)', [followerId, userId, 'follow', status]);
  notificationService.create({ recipient_id: userId, sender_id: followerId, type: status === 'pending' ? 'follow_request' : 'follow', entity_type: 'user', entity_id: followerId }).catch(()=>{});
  await feedService.invalidate(followerId);
  res.json({ following: status === 'accepted', requested: status === 'pending', status });
});

router.delete('/follow/:userId', authenticate, async (req: AuthRequest, res: Response) => {
  await db.query('DELETE FROM relationships WHERE follower_id = $1 AND following_id = $2 AND type = $3', [req.user!.id, req.params.userId, 'follow']);
  await feedService.invalidate(req.user!.id);
  res.json({ following: false });
});

router.post('/follow-requests/:userId/accept', authenticate, async (req: AuthRequest, res: Response) => {
  const r = await db.queryOne(`UPDATE relationships SET status = 'accepted' WHERE follower_id = $1 AND following_id = $2 AND type = 'follow' AND status = 'pending' RETURNING id`, [req.params.userId, req.user!.id]);
  if (!r) throw new NotFoundError('Follow request');
  notificationService.create({ recipient_id: req.params.userId, sender_id: req.user!.id, type: 'follow_accepted', entity_type: 'user', entity_id: req.user!.id }).catch(()=>{});
  res.json({ accepted: true });
});

router.delete('/follow-requests/:userId/decline', authenticate, async (req: AuthRequest, res: Response) => {
  await db.query(`DELETE FROM relationships WHERE follower_id = $1 AND following_id = $2 AND type = 'follow' AND status = 'pending'`, [req.params.userId, req.user!.id]);
  res.json({ declined: true });
});

router.get('/follow-requests', authenticate, async (req: AuthRequest, res: Response) => {
  const requests = await db.queryMany(
    `SELECT u.id, u.username, u.full_name, u.avatar_url, u.is_verified, r.created_at AS requested_at
     FROM relationships r JOIN users u ON u.id = r.follower_id
     WHERE r.following_id = $1 AND r.type = 'follow' AND r.status = 'pending' ORDER BY r.created_at DESC`,
    [req.user!.id]
  );
  res.json({ requests });
});

router.post('/block/:userId', authenticate, async (req: AuthRequest, res: Response) => {
  const { userId } = req.params;
  if (userId === req.user!.id) throw new AppError('Cannot block yourself', 400);
  await db.transaction(async (client) => {
    await client.query('DELETE FROM relationships WHERE (follower_id = $1 AND following_id = $2) OR (follower_id = $2 AND following_id = $1)', [req.user!.id, userId]);
    await client.query('INSERT INTO relationships (follower_id, following_id, type) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING', [req.user!.id, userId, 'blocked']);
  });
  await feedService.invalidate(req.user!.id);
  res.json({ blocked: true });
});

router.delete('/block/:userId', authenticate, async (req: AuthRequest, res: Response) => {
  await db.query('DELETE FROM relationships WHERE follower_id = $1 AND following_id = $2 AND type = $3', [req.user!.id, req.params.userId, 'blocked']);
  res.json({ blocked: false });
});

router.post('/mute/:userId', authenticate, async (req: AuthRequest, res: Response) => {
  await db.query('INSERT INTO relationships (follower_id, following_id, type) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING', [req.user!.id, req.params.userId, 'muted']);
  await feedService.invalidate(req.user!.id);
  res.json({ muted: true });
});

router.delete('/mute/:userId', authenticate, async (req: AuthRequest, res: Response) => {
  await db.query('DELETE FROM relationships WHERE follower_id = $1 AND following_id = $2 AND type = $3', [req.user!.id, req.params.userId, 'muted']);
  res.json({ muted: false });
});

router.get('/blocked', authenticate, async (req: AuthRequest, res: Response) => {
  const users = await db.queryMany(
    `SELECT u.id, u.username, u.full_name, u.avatar_url, r.created_at AS blocked_at
     FROM relationships r JOIN users u ON u.id = r.following_id
     WHERE r.follower_id = $1 AND r.type = 'blocked' ORDER BY r.created_at DESC`,
    [req.user!.id]
  );
  res.json({ blocked_users: users });
});

export default router;
