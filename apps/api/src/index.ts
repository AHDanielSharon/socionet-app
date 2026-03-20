import 'express-async-errors';
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import morgan from 'morgan';
import cookieParser from 'cookie-parser';
import { createServer } from 'http';

import { config } from '@config/index';
import { db } from '@lib/db';
import { initRedis } from '@lib/redis';
import { initStorage } from '@lib/storage';
import { initEmail } from '@lib/email';
import { logger, httpLogStream } from '@utils/logger';
import { errorHandler, notFoundHandler } from '@middleware/errorHandler';
import { rateLimiter } from '@middleware/rateLimiter';

// Routes
import authRoutes from '@routes/auth';
import userRoutes from '@routes/users';
import postRoutes from '@routes/posts';
import storyRoutes from '@routes/stories';
import feedRoutes from '@routes/feed';
import messageRoutes from '@routes/messages';
import conversationRoutes from '@routes/conversations';
import notificationRoutes from '@routes/notifications';
import communityRoutes from '@routes/communities';
import searchRoutes from '@routes/search';
import mediaRoutes from '@routes/media';
import exploreRoutes from '@routes/explore';
import liveRoutes from '@routes/live';
import callRoutes from '@routes/calls';
import walletRoutes from '@routes/wallet';
import aiRoutes from '@routes/ai';
import relationshipRoutes from '@routes/relationships';
import locationRoutes from '@routes/location';
import creatorRoutes from '@routes/creator';
import settingsRoutes from '@routes/settings';
import reportRoutes from '@routes/reports';

const app = express();
const httpServer = createServer(app);

async function bootstrap() {
  // ── Init services
  await initRedis();
  await initStorage();
  initEmail();

  // ── Security
  app.use(helmet({
    contentSecurityPolicy: false, // Handled by Next.js
    crossOriginResourcePolicy: { policy: 'cross-origin' },
  }));

  // ── CORS
  app.use(cors({
    origin: (origin, callback) => {
      const allowed = [config.app.url, 'http://localhost:3000', 'http://localhost:3001'];
      if (!origin || allowed.includes(origin)) callback(null, true);
      else callback(new Error('Not allowed by CORS'));
    },
    credentials: true,
    methods: ['GET','POST','PUT','PATCH','DELETE','OPTIONS'],
    allowedHeaders: ['Content-Type','Authorization','X-Refresh-Token','X-Request-ID'],
    exposedHeaders: ['X-Total-Count','X-Has-More'],
  }));

  app.use(compression({ level: 6 }));
  app.use(express.json({ limit: '10mb' }));
  app.use(express.urlencoded({ extended: true, limit: '10mb' }));
  app.use(cookieParser());

  // ── Logging
  if (!config.app.isProd) {
    app.use(morgan('dev'));
  } else {
    app.use(morgan('combined', { stream: httpLogStream }));
  }

  // ── Rate limiting
  app.use('/api/', rateLimiter);

  // ── Request ID
  app.use((req, _res, next) => {
    req.headers['x-request-id'] = req.headers['x-request-id'] || `req_${Date.now()}`;
    next();
  });

  // ── Health check
  app.get('/health', async (_req, res) => {
    const [dbOk] = await Promise.all([db.healthCheck()]);
    res.json({
      status: dbOk ? 'ok' : 'degraded',
      timestamp: new Date().toISOString(),
      version: config.app.version,
      environment: config.app.env,
      services: {
        database: dbOk ? 'ok' : 'error',
        redis: 'ok',
        storage: 'ok',
      },
    });
  });

  // ── API Routes
  const v1 = '/api/v1';
  app.use(`${v1}/auth`, authRoutes);
  app.use(`${v1}/users`, userRoutes);
  app.use(`${v1}/posts`, postRoutes);
  app.use(`${v1}/stories`, storyRoutes);
  app.use(`${v1}/feed`, feedRoutes);
  app.use(`${v1}/messages`, messageRoutes);
  app.use(`${v1}/conversations`, conversationRoutes);
  app.use(`${v1}/notifications`, notificationRoutes);
  app.use(`${v1}/communities`, communityRoutes);
  app.use(`${v1}/search`, searchRoutes);
  app.use(`${v1}/media`, mediaRoutes);
  app.use(`${v1}/explore`, exploreRoutes);
  app.use(`${v1}/live`, liveRoutes);
  app.use(`${v1}/calls`, callRoutes);
  app.use(`${v1}/wallet`, walletRoutes);
  app.use(`${v1}/ai`, aiRoutes);
  app.use(`${v1}/relationships`, relationshipRoutes);
  app.use(`${v1}/location`, locationRoutes);
  app.use(`${v1}/creator`, creatorRoutes);
  app.use(`${v1}/settings`, settingsRoutes);
  app.use(`${v1}/reports`, reportRoutes);

  // ── Error handling
  app.use(notFoundHandler);
  app.use(errorHandler);

  // ── Start
  httpServer.listen(config.server.port, () => {
    logger.info(`🚀 SOCIONET API running`, {
      port: config.server.port,
      env: config.app.env,
      url: `http://localhost:${config.server.port}`,
    });
  });

  // ── Graceful shutdown
  const shutdown = async (signal: string) => {
    logger.info(`${signal} received — shutting down gracefully`);
    httpServer.close(() => {
      db.end().then(() => {
        logger.info('Database pool closed');
        process.exit(0);
      });
    });
    setTimeout(() => process.exit(1), 30000);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('unhandledRejection', (reason) => {
    logger.error('Unhandled rejection', { reason: String(reason) });
  });
}

bootstrap().catch(err => {
  logger.error('Failed to start server', { error: String(err) });
  process.exit(1);
});

export default app;
