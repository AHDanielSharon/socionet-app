import { Router, Response } from 'express';
import { authenticate, optionalAuth, AuthRequest } from '@middleware/auth';
import { validate } from '@middleware/validate';
import { searchQuerySchema } from '@validators/schemas';
import { searchService } from '@services/searchService';
import { db } from '@lib/db';

const router = Router();

router.get('/', optionalAuth, async (req: AuthRequest, res: Response) => {
  const { q, type = 'all', limit = '20' } = req.query;
  if (!q) return res.status(400).json({ error: 'Query required' });
  if (req.user?.id) searchService.saveHistory(req.user.id, q as string).catch(() => {});
  const results = await searchService.global(q as string, type as string, req.user?.id || null, parseInt(limit as string));
  res.json({ query: q, results });
});

router.get('/trending', async (_req, res) => {
  const data = await searchService.trending();
  res.json(data);
});

router.get('/hashtag/:name', optionalAuth, async (req: AuthRequest, res: Response) => {
  const { name } = req.params;
  const { limit = '24' } = req.query;
  const hashtag = await db.queryOne('SELECT * FROM hashtags WHERE LOWER(name) = LOWER($1)', [name]);
  if (!hashtag) return res.status(404).json({ error: 'Hashtag not found' });
  const posts = await db.queryMany(
    `SELECT p.*, u.username, u.full_name, u.avatar_url, u.is_verified,
            (SELECT json_build_object('url', m.url, 'type', m.type, 'thumbnail_url', m.thumbnail_url, 'width', m.width, 'height', m.height, 'blurhash', m.blurhash)
             FROM post_media pm JOIN media m ON m.id = pm.media_id WHERE pm.post_id = p.id AND pm.position = 0 LIMIT 1) AS first_media
     FROM post_hashtags ph JOIN posts p ON p.id = ph.post_id JOIN users u ON u.id = p.user_id
     WHERE ph.hashtag_id = $1 AND p.visibility = 'public' AND u.status = 'active'
     ORDER BY p.likes_count DESC, p.created_at DESC LIMIT $2`,
    [hashtag.id, limit]
  );
  res.json({ hashtag, posts });
});

router.get('/history', authenticate, async (req: AuthRequest, res: Response) => {
  const history = await searchService.getHistory(req.user!.id);
  res.json({ history });
});

router.delete('/history', authenticate, async (req: AuthRequest, res: Response) => {
  await db.query('DELETE FROM search_history WHERE user_id = $1', [req.user!.id]);
  res.json({ cleared: true });
});

export default router;
