import 'express-async-errors';
import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import compression from 'compression';
import morgan from 'morgan';
import cookieParser from 'cookie-parser';
import { createServer } from 'http';

// ✅ FIXED IMPORTS (NO @)
import { config } from '../config/index';
import { db } from '../lib/db';
import { initRedis } from '../lib/redis';
import { initStorage } from '../lib/storage';
import { initEmail } from '../lib/email';
import { logger, httpLogStream } from '../utils/logger';
import { errorHandler, notFoundHandler } from '../middleware/errorHandler';
import { rateLimiter } from '../middleware/rateLimiter';

// Routes
import authRoutes from '../routes/auth';
import userRoutes from '../routes/users';
import postRoutes from '../routes/posts';
import storyRoutes from '../routes/stories';
import feedRoutes from '../routes/feed';
import messageRoutes from '../routes/messages';
import conversationRoutes from '../routes/conversations';
import notificationRoutes from '../routes/notifications';
import communityRoutes from '../routes/communities';
import searchRoutes from '../routes/search';
import mediaRoutes from '../routes/media';
import exploreRoutes from '../routes/explore';
import liveRoutes from '../routes/live';
import callRoutes from '../routes/calls';
import walletRoutes from '../routes/wallet';
import aiRoutes from '../routes/ai';
import relationshipRoutes from '../routes/relationships';
import locationRoutes from '../routes/location';
import creatorRoutes from '../routes/creator';
import settingsRoutes from '../routes/settings';
import reportRoutes from '../routes/reports';

const app = express();
const httpServer = createServer(app);

async function bootstrap() {
  await initRedis();
  await initStorage();
  initEmail();

  app.use(helmet({
    contentSecurityPolicy: false,
    crossOriginResourcePolicy: { policy: 'cross-origin' },
  }));

  app.use(cors({
    origin: (origin, callback) => {
      const allowed = [config.app.url, 'http://localhost:3000'];
      if (!origin || allowed.includes(origin)) callback(null, true);
      else callback(new Error('Not allowed by CORS'));
    },
    credentials: true,
  }));

  app.use(compression());
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  app.use(cookieParser());

  if (!config.app.isProd) {
    app.use(morgan('dev'));
  } else {
    app.use(morgan('combined', { stream: httpLogStream }));
  }

  app.use('/api/', rateLimiter);

  app.get('/health', async (_req, res) => {
    res.json({
      status: 'ok',
      time: new Date()
    });
  });

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

  app.use(notFoundHandler);
  app.use(errorHandler);

  httpServer.listen(config.server.port || 10000, () => {
    console.log(`🚀 Server running on port ${config.server.port}`);
  });
}

bootstrap();

export default app;