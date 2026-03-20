import { Router, Response } from 'express';
import { authenticate, optionalAuth, AuthRequest } from '@middleware/auth';
import { feedService } from '@services/feedService';

const router = Router();

router.get('/', authenticate, async (req: AuthRequest, res: Response) => {
  const { limit, cursor } = req.query;
  const data = await feedService.getHomeFeed(req.user!.id, { limit: parseInt(limit as string) || 20, cursor: cursor as string });
  res.json(data);
});

router.get('/reels', authenticate, async (req: AuthRequest, res: Response) => {
  const { limit, cursor } = req.query;
  const data = await feedService.getReelsFeed(req.user!.id, { limit: parseInt(limit as string) || 10, cursor: cursor as string });
  res.json(data);
});

router.get('/stories', authenticate, async (req: AuthRequest, res: Response) => {
  const data = await feedService.getStoriesFeed(req.user!.id);
  res.json(data);
});

router.get('/videos', optionalAuth, async (req: AuthRequest, res: Response) => {
  const { limit, offset } = req.query;
  const data = await feedService.getVideoFeed(req.user?.id || null, {
    limit: parseInt(limit as string) || 20,
    offset: parseInt(offset as string) || 0,
  });
  res.json(data);
});

export default router;
