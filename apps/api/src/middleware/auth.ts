import { Request, Response, NextFunction } from 'express';
import { verifyAccessToken, AccessTokenPayload } from '@lib/jwt';
import { db } from '@lib/db';
import { cache } from '@lib/redis';

export interface AuthRequest extends Request {
  user?: {
    id: string;
    username: string;
    is_verified: boolean;
    is_business: boolean;
    is_creator: boolean;
    status: string;
  };
}

// ── Full authentication - rejects if no token
export const authenticate = async (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const token = extractToken(req);
    if (!token) {
      res.status(401).json({ error: 'Authentication required', code: 'NO_TOKEN' });
      return;
    }

    let payload: AccessTokenPayload;
    try {
      payload = await verifyAccessToken(token);
    } catch (err: any) {
      if (err.name === 'TokenExpiredError') {
        res.status(401).json({ error: 'Token expired', code: 'TOKEN_EXPIRED' });
      } else {
        res.status(401).json({ error: 'Invalid token', code: 'INVALID_TOKEN' });
      }
      return;
    }

    const user = await getUserFromCache(payload.sub);
    if (!user) {
      res.status(401).json({ error: 'User not found', code: 'USER_NOT_FOUND' });
      return;
    }

    if (user.status !== 'active') {
      const msg = user.status === 'suspended' ? 'Account suspended' : 'Account deactivated';
      res.status(403).json({ error: msg, code: 'ACCOUNT_INACTIVE' });
      return;
    }

    req.user = user;
    next();
  } catch (err) {
    res.status(500).json({ error: 'Authentication error' });
  }
};

// ── Optional authentication - continues if no token
export const optionalAuth = async (
  req: AuthRequest,
  _res: Response,
  next: NextFunction
): Promise<void> => {
  try {
    const token = extractToken(req);
    if (!token) return next();

    const payload = await verifyAccessToken(token).catch(() => null);
    if (!payload) return next();

    const user = await getUserFromCache(payload.sub);
    if (user?.status === 'active') req.user = user;

    next();
  } catch {
    next();
  }
};

// ── Require specific roles
export const requireVerified = (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): void => {
  if (!req.user?.is_verified) {
    res.status(403).json({ error: 'Email verification required', code: 'UNVERIFIED' });
    return;
  }
  next();
};

export const requireCreator = (
  req: AuthRequest,
  res: Response,
  next: NextFunction
): void => {
  if (!req.user?.is_creator && !req.user?.is_business) {
    res.status(403).json({ error: 'Creator account required', code: 'NOT_CREATOR' });
    return;
  }
  next();
};

// ── Helpers
function extractToken(req: Request): string | null {
  const authHeader = req.headers.authorization;
  if (authHeader?.startsWith('Bearer ')) return authHeader.substring(7);

  // Also check cookie
  const cookieToken = (req as any).cookies?.access_token;
  if (cookieToken) return cookieToken;

  return null;
}

async function getUserFromCache(userId: string) {
  const cacheKey = `user:auth:${userId}`;
  const cached = await cache.get<any>(cacheKey);
  if (cached) return cached;

  const result = await db.queryOne(
    `SELECT id, username, is_verified, is_business, is_creator, status
     FROM users WHERE id = $1 AND deleted_at IS NULL`,
    [userId]
  );

  if (result) await cache.set(cacheKey, result, 120);
  return result;
}

export const clearUserCache = async (userId: string): Promise<void> => {
  await cache.del(`user:auth:${userId}`);
};
