import cron from 'node-cron';
import { db } from '@lib/db';
import { logger } from '@utils/logger';

export const startCleanupJobs = () => {
  cron.schedule('0 * * * *', async () => {
    try {
      const r = await db.query("DELETE FROM posts WHERE type = 'story' AND expires_at < NOW()");
      if (r.rowCount) logger.info(`Cleaned ${r.rowCount} expired stories`);
    } catch {}
  });
  cron.schedule('*/5 * * * *', async () => {
    try { await db.query('DELETE FROM messages WHERE expires_at IS NOT NULL AND expires_at < NOW()'); } catch {}
  });
  cron.schedule('0 2 * * *', async () => {
    try {
      const r = await db.query('DELETE FROM refresh_tokens WHERE expires_at < NOW()');
      logger.info(`Cleaned ${r.rowCount} expired tokens`);
    } catch {}
  });
  logger.info('Cleanup jobs started');
};
