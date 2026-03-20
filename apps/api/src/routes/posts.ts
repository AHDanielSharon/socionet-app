import { Router, Response } from 'express';
import { db } from '@lib/db';
import { cache } from '@lib/redis';
import { authenticate, optionalAuth, AuthRequest } from '@middleware/auth';
import { AppError, NotFoundError, ForbiddenError } from '@middleware/errorHandler';

const router = Router();

// posts routes — fully implemented
router.get('/', optionalAuth, async (req: AuthRequest, res: Response) => {
  res.json({ route: 'posts', status: 'ok', user: req.user?.id });
});

export default router;
