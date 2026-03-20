import { z } from 'zod';

// ── Auth
export const registerSchema = z.object({
  full_name: z.string().min(2).max(100).trim(),
  username: z.string()
    .min(3).max(30)
    .regex(/^[a-zA-Z0-9_.]+$/, 'Only letters, numbers, _ and . allowed')
    .transform(v => v.toLowerCase()),
  email: z.string().email().optional().transform(v => v?.toLowerCase()),
  phone: z.string().regex(/^\+?[1-9]\d{7,14}$/).optional(),
  password: z.string().min(8).max(128),
}).refine(d => d.email || d.phone, {
  message: 'Either email or phone is required',
  path: ['email'],
});

export const loginSchema = z.object({
  identifier: z.string().min(1, 'Email, username, or phone required').trim(),
  password: z.string().min(1, 'Password required'),
  remember_device: z.boolean().optional(),
});

export const sendOtpSchema = z.object({
  identifier: z.string().min(1),
  purpose: z.enum(['register', 'login', 'reset_password', 'verify', 'change_email']),
});

export const verifyOtpSchema = z.object({
  identifier: z.string().min(1),
  otp: z.string().length(6).regex(/^\d{6}$/),
  purpose: z.enum(['register', 'login', 'reset_password', 'verify', 'change_email']),
});

export const resetPasswordSchema = z.object({
  email: z.string().email(),
  otp: z.string().length(6),
  new_password: z.string().min(8).max(128),
});

// ── Users
export const updateProfileSchema = z.object({
  full_name: z.string().min(2).max(100).optional(),
  display_name: z.string().max(100).optional(),
  bio: z.string().max(500).optional(),
  website: z.string().url().optional().or(z.literal('')),
  location: z.string().max(100).optional(),
  date_of_birth: z.string().optional(),
  gender: z.enum(['male','female','non_binary','prefer_not_to_say','other']).optional(),
  pronouns: z.string().max(50).optional(),
  is_private: z.boolean().optional(),
  allow_messages_from: z.enum(['everyone','followers','nobody']).optional(),
  allow_tags_from: z.enum(['everyone','followers','nobody']).optional(),
  show_activity_status: z.boolean().optional(),
  show_read_receipts: z.boolean().optional(),
});

export const updateUsernameSchema = z.object({
  username: z.string()
    .min(3).max(30)
    .regex(/^[a-zA-Z0-9_.]+$/)
    .transform(v => v.toLowerCase()),
});

// ── Posts
export const createPostSchema = z.object({
  caption: z.string().max(2200).optional(),
  type: z.enum(['text','image','video','carousel','reel','story','live','podcast']).optional(),
  visibility: z.enum(['public','followers','close_friends','only_me']).optional(),
  location_name: z.string().max(255).optional(),
  location_lat: z.number().min(-90).max(90).optional(),
  location_lng: z.number().min(-180).max(180).optional(),
  feeling: z.string().max(100).optional(),
  feeling_emoji: z.string().max(10).optional(),
  community_id: z.string().uuid().optional(),
  scheduled_at: z.string().datetime().optional(),
});

export const updatePostSchema = z.object({
  caption: z.string().max(2200).optional(),
  visibility: z.enum(['public','followers','close_friends','only_me']).optional(),
  location_name: z.string().max(255).optional(),
  comments_disabled: z.boolean().optional(),
  likes_hidden: z.boolean().optional(),
  shares_disabled: z.boolean().optional(),
});

export const addCommentSchema = z.object({
  content: z.string().min(1).max(1000).trim(),
  parent_id: z.string().uuid().optional(),
});

// ── Messages
export const sendMessageSchema = z.object({
  content: z.string().max(4096).optional(),
  type: z.enum(['text','image','video','audio','file','location','sticker','gif','poll','voice_note']).optional(),
  reply_to_id: z.string().uuid().optional(),
  metadata: z.record(z.any()).optional(),
  disappear_after: z.number().int().positive().optional(),
});

export const createConversationSchema = z.object({
  type: z.enum(['direct','group','channel','broadcast']).optional(),
  user_ids: z.array(z.string().uuid()).min(1).max(256),
  name: z.string().max(100).optional(),
  description: z.string().max(500).optional(),
});

// ── Communities
export const createCommunitySchema = z.object({
  name: z.string().min(3).max(100).trim(),
  description: z.string().max(2000).optional(),
  category: z.string().max(100).optional(),
  tags: z.array(z.string().max(50)).max(10).optional(),
  is_private: z.boolean().optional(),
  requires_approval: z.boolean().optional(),
  is_nsfw: z.boolean().optional(),
});

// ── Search
export const searchQuerySchema = z.object({
  q: z.string().min(1).max(200).trim(),
  type: z.enum(['all','users','posts','communities','hashtags']).optional(),
  limit: z.string().transform(Number).pipe(z.number().int().min(1).max(50)).optional(),
  cursor: z.string().optional(),
});

// ── Notifications
export const notifPrefsSchema = z.object({
  likes: z.boolean().optional(),
  comments: z.boolean().optional(),
  replies: z.boolean().optional(),
  follows: z.boolean().optional(),
  mentions: z.boolean().optional(),
  messages: z.boolean().optional(),
  live_started: z.boolean().optional(),
  story_reactions: z.boolean().optional(),
  push_enabled: z.boolean().optional(),
  quiet_hours_enabled: z.boolean().optional(),
  quiet_hours_start: z.string().regex(/^\d{2}:\d{2}$/).optional(),
  quiet_hours_end: z.string().regex(/^\d{2}:\d{2}$/).optional(),
});

// ── Wallet
export const tipSchema = z.object({
  recipient_id: z.string().uuid(),
  amount_tokens: z.number().int().positive().max(1_000_000),
  message: z.string().max(200).optional(),
});

// ── AI
export const aiMessageSchema = z.object({
  content: z.string().min(1).max(4000).trim(),
});

export const aiQuickSchema = z.object({
  task: z.enum(['caption','bio','hashtags','reply','post_ideas','content_plan','analyze']),
  context: z.string().min(1).max(2000),
});

// ── Live
export const startLiveSchema = z.object({
  title: z.string().min(3).max(255).trim(),
  description: z.string().max(1000).optional(),
  category: z.string().max(100).optional(),
  is_recorded: z.boolean().optional(),
});

// ── Pagination
export const paginationSchema = z.object({
  limit: z.string().transform(Number).pipe(z.number().int().min(1).max(100)).optional(),
  cursor: z.string().optional(),
  offset: z.string().transform(Number).pipe(z.number().int().min(0)).optional(),
});
