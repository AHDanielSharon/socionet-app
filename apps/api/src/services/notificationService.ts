import { db } from '@lib/db';
import { publish, cache } from '@lib/redis';
import { logger } from '@utils/logger';

interface CreateNotifInput {
  recipient_id: string;
  sender_id?: string;
  type: string;
  entity_type?: string;
  entity_id?: string;
  title?: string;
  body?: string;
  action_url?: string;
  data?: Record<string, any>;
}

interface NotifTemplate {
  title: string;
  body: string;
  action_url?: string;
}

const templates: Record<string, (senderName: string, extra?: any) => NotifTemplate> = {
  like:            (n) => ({ title: 'New Like',         body: `${n} liked your post`,                    action_url: `/post/{entity_id}` }),
  comment:         (n) => ({ title: 'New Comment',      body: `${n} commented on your post`,             action_url: `/post/{entity_id}` }),
  reply:           (n) => ({ title: 'New Reply',        body: `${n} replied to your comment`,            action_url: `/post/{entity_id}` }),
  follow:          (n) => ({ title: 'New Follower',     body: `${n} started following you`,              action_url: `/profile/${n}` }),
  follow_request:  (n) => ({ title: 'Follow Request',   body: `${n} wants to follow you`,                action_url: `/notifications` }),
  follow_accepted: (n) => ({ title: 'Follow Accepted',  body: `${n} accepted your follow request`,       action_url: `/profile/${n}` }),
  mention:         (n) => ({ title: 'You were mentioned', body: `${n} mentioned you in a post`,          action_url: `/post/{entity_id}` }),
  dm:              (n) => ({ title: 'New Message',      body: `${n} sent you a message`,                 action_url: `/messages` }),
  group_message:   (n) => ({ title: 'New Group Message', body: `${n} sent a message`,                   action_url: `/messages` }),
  live_started:    (n) => ({ title: '🔴 Live Now',      body: `${n} just started a live stream`,        action_url: `/live` }),
  story_reaction:  (n) => ({ title: 'Story Reaction',   body: `${n} reacted to your story`,             action_url: `/` }),
  comment_liked:   (n) => ({ title: 'Comment Liked',    body: `${n} liked your comment`,                 action_url: `/post/{entity_id}` }),
  tip_received:    (n, extra) => ({ title: '💰 Tip Received', body: `${n} sent you ${extra?.amount || ''} tokens`, action_url: `/wallet` }),
  achievement:     (_) => ({ title: '🏆 Achievement Unlocked', body: `You earned a new badge!`,         action_url: `/profile` }),
  community_invite:(n) => ({ title: 'Community Invite', body: `${n} invited you to join a community`,   action_url: `/communities` }),
};

const PREFS_CACHE_TTL = 300;

export const notificationService = {
  create: async (input: CreateNotifInput): Promise<void> => {
    try {
      // Don't notify yourself
      if (input.sender_id && input.sender_id === input.recipient_id) return;

      // Check preferences
      const canNotify = await notificationService.checkPrefs(input.recipient_id, input.type);
      if (!canNotify) return;

      // Build title/body if not provided
      let { title, body, action_url } = input;
      if ((!title || !body) && input.sender_id) {
        const sender = await db.queryOne<{ full_name: string; username: string }>(
          'SELECT full_name, username FROM users WHERE id = $1',
          [input.sender_id]
        );
        if (sender && templates[input.type]) {
          const tmpl = templates[input.type](sender.full_name, input.data);
          title = title || tmpl.title;
          body = body || tmpl.body;
          action_url = action_url || tmpl.action_url?.replace('{entity_id}', input.entity_id || '');
        }
      }

      // Insert notification
      const result = await db.queryOne<{ id: string }>(
        `INSERT INTO notifications
           (recipient_id, sender_id, type, entity_type, entity_id, title, body, action_url, data)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         RETURNING id`,
        [
          input.recipient_id, input.sender_id || null, input.type,
          input.entity_type || null, input.entity_id || null,
          title || null, body || null, action_url || null,
          JSON.stringify(input.data || {}),
        ]
      );

      if (!result) return;

      // Publish for real-time delivery
      await publish('notifications', {
        id: result.id,
        recipient_id: input.recipient_id,
        sender_id: input.sender_id,
        type: input.type,
        title,
        body,
        action_url,
        entity_type: input.entity_type,
        entity_id: input.entity_id,
        created_at: new Date().toISOString(),
      });

      // Send push notification
      await notificationService.sendPush(input.recipient_id, title || '', body || '', action_url);

    } catch (err) {
      logger.error('Failed to create notification', { error: String(err), input });
    }
  },

  createBulk: async (
    recipientIds: string[],
    input: Omit<CreateNotifInput, 'recipient_id'>
  ): Promise<void> => {
    const BATCH = 50;
    for (let i = 0; i < recipientIds.length; i += BATCH) {
      const batch = recipientIds.slice(i, i + BATCH);
      await Promise.allSettled(
        batch.map(rid => notificationService.create({ ...input, recipient_id: rid }))
      );
    }
  },

  checkPrefs: async (userId: string, type: string): Promise<boolean> => {
    const cacheKey = `notif_prefs:${userId}`;
    let prefs = await cache.get<any>(cacheKey);

    if (!prefs) {
      prefs = await db.queryOne(
        'SELECT * FROM notification_preferences WHERE user_id = $1',
        [userId]
      );
      if (prefs) await cache.set(cacheKey, prefs, PREFS_CACHE_TTL);
    }

    if (!prefs) return true; // default to allow

    const prefMap: Record<string, string> = {
      like: 'likes', comment: 'comments', reply: 'replies',
      follow: 'follows', follow_request: 'follows', follow_accepted: 'follows',
      mention: 'mentions', dm: 'messages', group_message: 'group_messages',
      live_started: 'live_started', story_reaction: 'story_reactions',
      tip_received: 'tips_received', achievement: 'achievements',
    };

    const prefKey = prefMap[type];
    return prefKey ? prefs[prefKey] !== false : true;
  },

  sendPush: async (
    userId: string,
    title: string,
    body: string,
    actionUrl?: string
  ): Promise<void> => {
    try {
      const tokens = await db.queryMany<{ token: string; platform: string }>(
        'SELECT token, platform FROM push_tokens WHERE user_id = $1',
        [userId]
      );

      if (!tokens.length || !process.env.FIREBASE_PROJECT_ID) return;

      const admin = await import('firebase-admin').then(m => m.default);
      if (!admin.apps.length) {
        admin.initializeApp({
          credential: admin.credential.cert({
            projectId: process.env.FIREBASE_PROJECT_ID,
            privateKey: process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, '\n'),
            clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
          } as any),
        });
      }

      const badTokens: string[] = [];

      await Promise.allSettled(tokens.map(async ({ token, platform }) => {
        try {
          await admin.messaging().send({
            token,
            notification: { title, body },
            data: { action_url: actionUrl || '/', click_action: 'FLUTTER_NOTIFICATION_CLICK' },
            android: {
              priority: 'high',
              notification: { channelId: 'socionet_default', priority: 'high', sound: 'default' },
            },
            apns: {
              payload: { aps: { badge: 1, sound: 'default', contentAvailable: true } },
              headers: { 'apns-priority': '10' },
            },
            webpush: {
              notification: { icon: '/icons/icon-192.png', badge: '/icons/badge.png' },
              fcmOptions: { link: actionUrl || '/' },
            },
          });
        } catch (err: any) {
          if (
            err.code === 'messaging/registration-token-not-registered' ||
            err.code === 'messaging/invalid-registration-token'
          ) {
            badTokens.push(token);
          }
        }
      }));

      // Remove invalid tokens
      if (badTokens.length > 0) {
        await db.query(
          'DELETE FROM push_tokens WHERE token = ANY($1)',
          [badTokens]
        );
      }
    } catch (err) {
      logger.error('Push notification error', { error: String(err) });
    }
  },

  getUnreadCount: async (userId: string): Promise<number> => {
    const result = await db.queryOne<{ count: string }>(
      'SELECT COUNT(*) FROM notifications WHERE recipient_id = $1 AND is_read = false',
      [userId]
    );
    return parseInt(result?.count || '0');
  },

  markAllRead: async (userId: string): Promise<void> => {
    await db.query(
      'UPDATE notifications SET is_read = true, read_at = NOW() WHERE recipient_id = $1 AND is_read = false',
      [userId]
    );
  },
};
