import cron from 'node-cron';
import { db } from '@lib/db';
import { sendNotificationDigest } from '@lib/email';
import { logger } from '@utils/logger';

export const startNotificationJobs = () => {
  cron.schedule('0 9 * * *', async () => {
    try {
      const users = await db.queryMany(
        `SELECT u.id, u.email, u.full_name, u.username FROM users u JOIN notification_preferences np ON np.user_id = u.id WHERE np.email_digest = true AND u.email IS NOT NULL AND u.status = 'active'`
      );
      for (const user of users) {
        const notifs = await db.queryMany(
          `SELECT title, body, created_at FROM notifications WHERE recipient_id = $1 AND is_read = false AND created_at > NOW() - INTERVAL '24 hours' ORDER BY created_at DESC LIMIT 10`,
          [user.id]
        );
        if (notifs.length > 0) await sendNotificationDigest(user.email, user.username, notifs.map(n=>({title:n.title||'',body:n.body||'',time:n.created_at}))).catch(()=>{});
      }
    } catch (err) { logger.error('Email digest failed', { error: String(err) }); }
  });
};
