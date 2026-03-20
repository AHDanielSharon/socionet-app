import rateLimit from 'express-rate-limit';
import { config } from '@config/index';

const makeOptions = (max: number, windowMs: number, message: string) => ({
  windowMs,
  max,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: message, code: 'RATE_LIMITED' },
  skip: (req: any) => req.method === 'OPTIONS',
});

export const rateLimiter = rateLimit(makeOptions(
  config.rateLimit.max,
  config.rateLimit.windowMs,
  'Too many requests, please slow down.'
));

export const authRateLimiter = rateLimit(makeOptions(
  config.rateLimit.authMax,
  15 * 60 * 1000,
  'Too many auth attempts. Try again in 15 minutes.'
));

export const uploadRateLimiter = rateLimit(makeOptions(
  config.rateLimit.uploadMax,
  60 * 1000,
  'Too many uploads. Please wait a moment.'
));

export const messageRateLimiter = rateLimit(makeOptions(
  config.rateLimit.messageMax,
  60 * 1000,
  'Sending messages too fast.'
));

export const searchRateLimiter = rateLimit(makeOptions(
  60,
  60 * 1000,
  'Search rate limit exceeded.'
));
