import { Router, Response } from 'express';
import { db } from '@lib/db';
import { authenticate, AuthRequest } from '@middleware/auth';
import { notificationService } from '@services/notificationService';

const router = Router();

router.get('/', authenticate, async (req: AuthRequest, res: Response) => {
  const { limit = '30', cursor, type } = req.query;
  const notifs = await db.queryMany(
    `SELECT n.*, u.username AS sender_username, u.full_name AS sender_name, u.avatar_url AS sender_avatar
     FROM notifications n LEFT JOIN users u ON u.id = n.sender_id
     WHERE n.recipient_id = $1 ${type ? `AND n.type = '${type}'` : ''} ${cursor ? `AND n.created_at < '${cursor}'` : ''}
     ORDER BY n.created_at DESC LIMIT $2`,
    [req.user!.id, limit]
  );
  const unreadCount = await notificationService.getUnreadCount(req.user!.id);
  res.json({ notifications: notifs, unread_count: unreadCount, has_more: notifs.length === parseInt(limit as string) });
});

router.get('/unread-count', authenticate, async (req: AuthRequest, res: Response) => {
  const count = await notificationService.getUnreadCount(req.user!.id);
  res.json({ count });
});

router.post('/mark-read', authenticate, async (req: AuthRequest, res: Response) => {
  const { notification_ids } = req.body;
  if (notification_ids?.length) {
    await db.query('UPDATE notifications SET is_read = true, read_at = NOW() WHERE id = ANY($1) AND recipient_id = $2', [notification_ids, req.user!.id]);
  } else {
    await notificationService.markAllRead(req.user!.id);
  }
  res.json({ success: true });
});

router.put('/preferences', authenticate, async (req: AuthRequest, res: Response) => {
  const { likes, comments, replies, follows, mentions, messages, live_started, story_reactions, push_enabled, email_digest, quiet_hours_enabled, quiet_hours_start, quiet_hours_end } = req.body;
  await db.query(
    `INSERT INTO notification_preferences (user_id, likes, comments, replies, follows, mentions, messages, live_started, story_reactions, push_enabled, email_digest, quiet_hours_enabled, quiet_hours_start, quiet_hours_end)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
     ON CONFLICT (user_id) DO UPDATE SET likes=COALESCE($2,notification_preferences.likes), comments=COALESCE($3,notification_preferences.comments), replies=COALESCE($4,notification_preferences.replies), follows=COALESCE($5,notification_preferences.follows), mentions=COALESCE($6,notification_preferences.mentions), messages=COALESCE($7,notification_preferences.messages), live_started=COALESCE($8,notification_preferences.live_started), story_reactions=COALESCE($9,notification_preferences.story_reactions), push_enabled=COALESCE($10,notification_preferences.push_enabled), email_digest=COALESCE($11,notification_preferences.email_digest), quiet_hours_enabled=COALESCE($12,notification_preferences.quiet_hours_enabled), quiet_hours_start=COALESCE($13,notification_preferences.quiet_hours_start), quiet_hours_end=COALESCE($14,notification_preferences.quiet_hours_end), updated_at=NOW()`,
    [req.user!.id, likes, comments, replies, follows, mentions, messages, live_started, story_reactions, push_enabled, email_digest, quiet_hours_enabled, quiet_hours_start||null, quiet_hours_end||null]
  );
  res.json({ updated: true });
});

router.post('/push-token', authenticate, async (req: AuthRequest, res: Response) => {
  const { token, platform, device_id } = req.body;
  if (!token) return res.status(400).json({ error: 'Token required' });
  await db.query('INSERT INTO push_tokens (user_id, token, platform, device_id) VALUES ($1, $2, $3, $4) ON CONFLICT (token) DO UPDATE SET user_id = $1, updated_at = NOW()', [req.user!.id, token, platform||'web', device_id||null]);
  res.json({ registered: true });
});

router.delete('/:id', authenticate, async (req: AuthRequest, res: Response) => {
  await db.query('DELETE FROM notifications WHERE id = $1 AND recipient_id = $2', [req.params.id, req.user!.id]);
  res.json({ deleted: true });
});

export default router;
