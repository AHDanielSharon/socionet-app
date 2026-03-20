import jwt, { SignOptions } from 'jsonwebtoken';
import { v4 as uuidv4 } from 'uuid';
import { config } from '@config/index';
import { tokenBlacklist } from '@lib/redis';

export interface AccessTokenPayload {
  sub: string;
  jti: string;
  username: string;
  iat?: number;
  exp?: number;
}

export interface RefreshTokenPayload {
  sub: string;
  jti: string;
  iat?: number;
  exp?: number;
}

export const generateAccessToken = (userId: string, username: string): string => {
  const payload: Omit<AccessTokenPayload, 'iat' | 'exp'> = {
    sub: userId,
    jti: uuidv4(),
    username,
  };
  return jwt.sign(payload, config.jwt.secret, {
    expiresIn: config.jwt.accessExpiry,
  } as SignOptions);
};

export const generateRefreshToken = (userId: string): { token: string; jti: string } => {
  const jti = uuidv4();
  const payload: Omit<RefreshTokenPayload, 'iat' | 'exp'> = { sub: userId, jti };
  const token = jwt.sign(payload, config.jwt.refreshSecret, {
    expiresIn: config.jwt.refreshExpiry,
  } as SignOptions);
  return { token, jti };
};

export const verifyAccessToken = async (token: string): Promise<AccessTokenPayload> => {
  const decoded = jwt.verify(token, config.jwt.secret) as AccessTokenPayload;
  if (await tokenBlacklist.isBlacklisted(decoded.jti)) {
    throw new Error('Token has been revoked');
  }
  return decoded;
};

export const verifyRefreshToken = (token: string): RefreshTokenPayload => {
  return jwt.verify(token, config.jwt.refreshSecret) as RefreshTokenPayload;
};

export const decodeToken = <T = any>(token: string): T | null => {
  try { return jwt.decode(token) as T; } catch { return null; }
};

export const getExpirySeconds = (expiry: string): number => {
  const units: Record<string, number> = { s: 1, m: 60, h: 3600, d: 86400, w: 604800 };
  const match = expiry.match(/^(\d+)([smhdw])$/);
  if (!match) return 86400;
  return parseInt(match[1]) * (units[match[2]] || 86400);
};
