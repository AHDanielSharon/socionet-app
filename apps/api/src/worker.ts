import 'dotenv/config';
import { initRedis } from '@lib/redis';
import { logger } from '@utils/logger';
import { startCleanupJobs } from '@jobs/cleanupJob';
import { startNotificationJobs } from '@jobs/notificationJob';

async function startWorker() {
  await initRedis();
  startCleanupJobs();
  startNotificationJobs();
  logger.info('Worker started');
  process.on('SIGTERM', () => process.exit(0));
}
startWorker().catch(err => { logger.error('Worker failed', { error: String(err) }); process.exit(1); });
