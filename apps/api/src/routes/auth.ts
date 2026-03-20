import { Router, Request, Response } from 'express';
import bcrypt from 'bcryptjs';
import crypto from 'crypto';
import { db } from '@lib/db';
import { cache } from '@lib/redis';
import { generateAccessToken, generateRefreshToken, verifyRefreshToken, getExpirySeconds, tokenBlacklist } from '@lib/jwt';
import { sendOTPEmail, sendWelcomeEmail, sendPasswordChangedEmail, sendNewLoginAlert } from '@lib/email';
import { authenticate, AuthRequest, clearUserCache } from '@middleware/auth';
import { authRateLimiter } from '@middleware/rateLimiter';
import { validate } from '@middleware/validate';
import { registerSchema, loginSchema, sendOtpSchema, verifyOtpSchema, resetPasswordSchema } from '@validators/schemas';
import { AppError, ConflictError, ValidationError } from '@middleware/errorHandler';

const router = Router();

const hashOTP = (otp: string) => crypto.createHash('sha256').update(otp).digest('hex');
const genOTP = (len = 6) => Math.floor(Math.pow(10, len-1) + Math.random() * 9 * Math.pow(10, len-1)).toString();

router.post('/register', authRateLimiter, validate(registerSchema), async (req: Request, res: Response) => {
  const { email, phone, username, full_name, password } = req.body;

  const existing = await db.queryOne(
    'SELECT id FROM users WHERE LOWER(username) = $1 OR (email IS NOT NULL AND LOWER(email) = $2)',
    [username, email?.toLowerCase() || '']
  );
  if (existing) throw new ConflictError('Username or email already taken');

  const password_hash = await bcrypt.hash(password, parseInt(process.env.BCRYPT_ROUNDS || '12'));

  const user = await db.transaction(async (client) => {
    const u = await client.query(
      `INSERT INTO users (username, email, phone, full_name, password_hash)
       VALUES ($1, $2, $3, $4, $5) RETURNING id, username, email, full_name, created_at`,
      [username, email || null, phone || null, full_name.trim(), password_hash]
    );
    const newUser = u.rows[0];
    await client.query('INSERT INTO wallets (user_id) VALUES ($1)', [newUser.id]);
    await client.query('INSERT INTO notification_preferences (user_id) VALUES ($1)', [newUser.id]);
    await client.query('INSERT INTO user_settings (user_id) VALUES ($1)', [newUser.id]);
    return newUser;
  });

  const accessToken = generateAccessToken(user.id, user.username);
  const { token: refreshToken } = generateRefreshToken(user.id);
  const rtHash = await bcrypt.hash(refreshToken, 10);
  await db.query(
    `INSERT INTO refresh_tokens (user_id, token_hash, expires_at, user_agent, ip_address)
     VALUES ($1, $2, NOW() + INTERVAL '30 days', $3, $4)`,
    [user.id, rtHash, req.headers['user-agent'] || 'unknown', req.ip]
  );

  if (email) {
    const otp = genOTP();
    await db.query(
      `INSERT INTO otp_codes (identifier, code_hash, purpose, expires_at)
       VALUES ($1, $2, 'register', NOW() + INTERVAL '10 minutes')`,
      [email.toLowerCase(), hashOTP(otp)]
    );
    sendOTPEmail(email, otp, 'register').catch(e => console.error('OTP email failed:', e));
    sendWelcomeEmail(email, user.username, user.full_name).catch(e => console.error('Welcome email failed:', e));
  }

  res.status(201).json({
    message: 'Account created successfully',
    user: { id: user.id, username: user.username, email: user.email, full_name: user.full_name, is_verified: false },
    tokens: { access_token: accessToken, refresh_token: refreshToken },
    requires_verification: !!email,
  });
});

router.post('/login', authRateLimiter, validate(loginSchema), async (req: Request, res: Response) => {
  const { identifier, password } = req.body;

  await db.query(
    'INSERT INTO login_attempts (identifier, ip_address, success) VALUES ($1, $2, false)',
    [identifier, req.ip]
  ).catch(() => {});

  const user = await db.queryOne(
    `SELECT id, username, email, full_name, avatar_url, password_hash, is_verified, status
     FROM users
     WHERE (LOWER(email) = LOWER($1) OR LOWER(username) = LOWER($1) OR phone = $1)
       AND deleted_at IS NULL`,
    [identifier]
  );

  if (!user) throw new AppError('Invalid credentials', 401, 'INVALID_CREDENTIALS');
  if (user.status === 'banned') throw new AppError('Account banned', 403, 'BANNED');
  if (user.status === 'suspended') throw new AppError('Account suspended. Contact support.', 403, 'SUSPENDED');

  const valid = await bcrypt.compare(password, user.password_hash);
  if (!valid) throw new AppError('Invalid credentials', 401, 'INVALID_CREDENTIALS');

  const accessToken = generateAccessToken(user.id, user.username);
  const { token: refreshToken } = generateRefreshToken(user.id);
  const rtHash = await bcrypt.hash(refreshToken, 10);

  await db.query(
    `INSERT INTO refresh_tokens (user_id, token_hash, expires_at, user_agent, ip_address)
     VALUES ($1, $2, NOW() + INTERVAL '30 days', $3, $4)`,
    [user.id, rtHash, req.headers['user-agent'] || 'unknown', req.ip]
  );

  await db.query('UPDATE users SET is_online = true, last_seen_at = NOW() WHERE id = $1', [user.id]);
  await db.query('UPDATE login_attempts SET success = true WHERE identifier = $1 AND ip_address = $2 ORDER BY created_at DESC LIMIT 1', [identifier, req.ip]).catch(() => {});

  res.json({
    user: { id: user.id, username: user.username, email: user.email, full_name: user.full_name, avatar_url: user.avatar_url, is_verified: user.is_verified },
    tokens: { access_token: accessToken, refresh_token: refreshToken },
  });
});

router.post('/logout', authenticate, async (req: AuthRequest, res: Response) => {
  const refreshToken = req.headers['x-refresh-token'] as string || req.body.refresh_token;
  if (refreshToken) {
    const tokens = await db.queryMany('SELECT id, token_hash FROM refresh_tokens WHERE user_id = $1', [req.user!.id]);
    for (const t of tokens) {
      if (await bcrypt.compare(refreshToken, t.token_hash)) {
        await db.query('DELETE FROM refresh_tokens WHERE id = $1', [t.id]);
        break;
      }
    }
  }
  await db.query('UPDATE users SET is_online = false, last_seen_at = NOW() WHERE id = $1', [req.user!.id]);
  await clearUserCache(req.user!.id);
  res.json({ message: 'Logged out successfully' });
});

router.post('/refresh', async (req: Request, res: Response) => {
  const refreshToken = req.headers['x-refresh-token'] as string || req.body.refresh_token;
  if (!refreshToken) throw new AppError('Refresh token required', 401, 'NO_REFRESH_TOKEN');

  let payload;
  try { payload = verifyRefreshToken(refreshToken); }
  catch { throw new AppError('Invalid or expired refresh token', 401, 'INVALID_REFRESH_TOKEN'); }

  const tokens = await db.queryMany('SELECT id, token_hash FROM refresh_tokens WHERE user_id = $1 AND expires_at > NOW()', [payload.sub]);
  let validTokenId: string | null = null;
  for (const t of tokens) {
    if (await bcrypt.compare(refreshToken, t.token_hash)) { validTokenId = t.id; break; }
  }
  if (!validTokenId) throw new AppError('Refresh token not found', 401, 'REFRESH_NOT_FOUND');

  await db.query('DELETE FROM refresh_tokens WHERE id = $1', [validTokenId]);

  const user = await db.queryOne('SELECT id, username, status FROM users WHERE id = $1', [payload.sub]);
  if (!user || user.status !== 'active') throw new AppError('User inactive', 401);

  const newAccess = generateAccessToken(user.id, user.username);
  const { token: newRefresh } = generateRefreshToken(user.id);
  const newHash = await bcrypt.hash(newRefresh, 10);
  await db.query(`INSERT INTO refresh_tokens (user_id, token_hash, expires_at, user_agent, ip_address) VALUES ($1, $2, NOW() + INTERVAL '30 days', $3, $4)`, [user.id, newHash, req.headers['user-agent'] || '', req.ip]);

  res.json({ tokens: { access_token: newAccess, refresh_token: newRefresh } });
});

router.post('/send-otp', authRateLimiter, validate(sendOtpSchema), async (req: Request, res: Response) => {
  const { identifier, purpose } = req.body;
  const count = await cache.incrEx(`otp_rate:${identifier}`, 600);
  if (count > 3) throw new AppError('Too many OTP requests. Try again in 10 minutes.', 429, 'OTP_RATE_LIMITED');

  const otp = genOTP();
  await db.query(
    `INSERT INTO otp_codes (identifier, code_hash, purpose, expires_at)
     VALUES ($1, $2, $3, NOW() + INTERVAL '10 minutes')`,
    [identifier.toLowerCase(), hashOTP(otp), purpose]
  );

  if (identifier.includes('@')) await sendOTPEmail(identifier, otp, purpose).catch(() => {});
  res.json({ message: 'OTP sent successfully' });
});

router.post('/verify-otp', authRateLimiter, validate(verifyOtpSchema), async (req: Request, res: Response) => {
  const { identifier, otp, purpose } = req.body;
  const code = await db.queryOne(
    `SELECT id FROM otp_codes WHERE LOWER(identifier) = LOWER($1) AND code_hash = $2 AND purpose = $3 AND expires_at > NOW() AND used_at IS NULL ORDER BY created_at DESC LIMIT 1`,
    [identifier, hashOTP(otp), purpose]
  );
  if (!code) throw new ValidationError('Invalid or expired OTP');

  await db.query('UPDATE otp_codes SET used_at = NOW() WHERE id = $1', [code.id]);
  if (purpose === 'register' || purpose === 'verify') {
    await db.query('UPDATE users SET is_verified = true WHERE LOWER(email) = LOWER($1)', [identifier]);
  }
  res.json({ message: 'Verified successfully', verified: true });
});

router.post('/forgot-password', authRateLimiter, async (req: Request, res: Response) => {
  const { email } = req.body;
  if (!email) throw new ValidationError('Email required');
  const user = await db.queryOne('SELECT id FROM users WHERE LOWER(email) = LOWER($1)', [email]);
  if (user) {
    const otp = genOTP();
    await db.query(`INSERT INTO otp_codes (identifier, code_hash, purpose, expires_at) VALUES ($1, $2, 'reset_password', NOW() + INTERVAL '10 minutes')`, [email.toLowerCase(), hashOTP(otp)]);
    await sendOTPEmail(email, otp, 'reset_password').catch(() => {});
  }
  res.json({ message: 'If this email exists, a reset code has been sent.' });
});

router.post('/reset-password', authRateLimiter, validate(resetPasswordSchema), async (req: Request, res: Response) => {
  const { email, otp, new_password } = req.body;
  const code = await db.queryOne(`SELECT id FROM otp_codes WHERE LOWER(identifier) = LOWER($1) AND code_hash = $2 AND purpose = 'reset_password' AND expires_at > NOW() AND used_at IS NULL`, [email, hashOTP(otp)]);
  if (!code) throw new ValidationError('Invalid or expired reset code');

  await db.query('UPDATE otp_codes SET used_at = NOW() WHERE id = $1', [code.id]);
  const hash = await bcrypt.hash(new_password, parseInt(process.env.BCRYPT_ROUNDS || '12'));
  await db.query('UPDATE users SET password_hash = $1, updated_at = NOW() WHERE LOWER(email) = LOWER($2)', [hash, email]);
  await db.query('DELETE FROM refresh_tokens WHERE user_id = (SELECT id FROM users WHERE LOWER(email) = LOWER($1))', [email]);
  await sendPasswordChangedEmail(email, email.split('@')[0]).catch(() => {});

  res.json({ message: 'Password reset successfully' });
});

router.get('/me', authenticate, async (req: AuthRequest, res: Response) => {
  const user = await db.queryOne(
    `SELECT u.*, s.theme, s.accent_color, s.language, s.autoplay_videos, s.compact_mode
     FROM users u LEFT JOIN user_settings s ON s.user_id = u.id
     WHERE u.id = $1`,
    [req.user!.id]
  );
  if (!user) throw new AppError('User not found', 404);
  delete user.password_hash;
  res.json({ user });
});

export default router;
