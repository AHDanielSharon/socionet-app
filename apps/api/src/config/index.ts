import 'dotenv/config';

const required = (key: string): string => {
  const val = process.env[key];
  if (!val) throw new Error(`Missing required env var: ${key}`);
  return val;
};

const optional = (key: string, defaultVal = ''): string =>
  process.env[key] || defaultVal;

export const config = {
  app: {
    name: optional('APP_NAME', 'SOCIONET'),
    url: optional('APP_URL', 'http://localhost:3000'),
    version: optional('APP_VERSION', '1.0.0'),
    env: optional('NODE_ENV', 'development'),
    isDev: optional('NODE_ENV', 'development') === 'development',
    isProd: optional('NODE_ENV', 'development') === 'production',
  },

  server: {
    port: parseInt(optional('API_PORT', '4000')),
  },

  db: {
    url: optional('DATABASE_URL', 'postgresql://socionet:socionet@localhost:5432/socionet'),
    poolMin: parseInt(optional('DATABASE_POOL_MIN', '2')),
    poolMax: parseInt(optional('DATABASE_POOL_MAX', '20')),
    ssl: optional('DATABASE_SSL', 'false') === 'true',
  },

  redis: {
    url: optional('REDIS_URL', 'redis://localhost:6379'),
    prefix: optional('REDIS_KEY_PREFIX', 'socionet:'),
  },

  jwt: {
    secret: optional('JWT_SECRET', 'dev_secret_change_in_production'),
    refreshSecret: optional('JWT_REFRESH_SECRET', 'dev_refresh_secret'),
    accessExpiry: optional('JWT_ACCESS_EXPIRY', '15m'),
    refreshExpiry: optional('JWT_REFRESH_EXPIRY', '30d'),
  },

  bcrypt: {
    rounds: parseInt(optional('BCRYPT_ROUNDS', '12')),
  },

  otp: {
    expiryMinutes: parseInt(optional('OTP_EXPIRY_MINUTES', '10')),
    length: parseInt(optional('OTP_LENGTH', '6')),
  },

  minio: {
    endpoint: optional('MINIO_ENDPOINT', 'localhost'),
    port: parseInt(optional('MINIO_PORT', '9000')),
    useSSL: optional('MINIO_USE_SSL', 'false') === 'true',
    accessKey: optional('MINIO_ACCESS_KEY', 'minioadmin'),
    secretKey: optional('MINIO_SECRET_KEY', 'minioadmin'),
    buckets: {
      media: optional('MINIO_BUCKET_MEDIA', 'socionet-media'),
      avatars: optional('MINIO_BUCKET_AVATARS', 'socionet-avatars'),
      videos: optional('MINIO_BUCKET_VIDEOS', 'socionet-videos'),
      audio: optional('MINIO_BUCKET_AUDIO', 'socionet-audio'),
    },
    publicUrl: optional('NEXT_PUBLIC_MINIO_URL', 'http://localhost:9000'),
  },

  email: {
    host: optional('SMTP_HOST', 'smtp.gmail.com'),
    port: parseInt(optional('SMTP_PORT', '587')),
    secure: optional('SMTP_SECURE', 'false') === 'true',
    user: optional('SMTP_USER', ''),
    pass: optional('SMTP_PASS', ''),
    from: optional('EMAIL_FROM', 'SOCIONET <noreply@socionet.app>'),
    fromName: optional('EMAIL_FROM_NAME', 'SOCIONET'),
  },

  openai: {
    apiKey: optional('OPENAI_API_KEY', ''),
    model: optional('OPENAI_MODEL', 'gpt-4o-mini'),
    maxTokens: parseInt(optional('OPENAI_MAX_TOKENS', '800')),
    temperature: parseFloat(optional('OPENAI_TEMPERATURE', '0.7')),
  },

  firebase: {
    projectId: optional('FIREBASE_PROJECT_ID', ''),
    privateKey: optional('FIREBASE_PRIVATE_KEY', '').replace(/\\n/g, '\n'),
    clientEmail: optional('FIREBASE_CLIENT_EMAIL', ''),
  },

  rateLimit: {
    windowMs: parseInt(optional('RATE_LIMIT_WINDOW_MS', '900000')),
    max: parseInt(optional('RATE_LIMIT_MAX', '200')),
    authMax: parseInt(optional('AUTH_RATE_LIMIT_MAX', '10')),
    uploadMax: parseInt(optional('UPLOAD_RATE_LIMIT_MAX', '30')),
    messageMax: parseInt(optional('MESSAGE_RATE_LIMIT_MAX', '60')),
  },

  upload: {
    maxImageMb: parseInt(optional('MAX_IMAGE_SIZE_MB', '10')),
    maxVideoMb: parseInt(optional('MAX_VIDEO_SIZE_MB', '500')),
    maxAudioMb: parseInt(optional('MAX_AUDIO_SIZE_MB', '50')),
    maxFileMb: parseInt(optional('MAX_FILE_SIZE_MB', '100')),
  },

  features: {
    ai: optional('FEATURE_AI_ASSISTANT', 'true') === 'true',
    wallet: optional('FEATURE_WALLET', 'true') === 'true',
    live: optional('FEATURE_LIVE_STREAMING', 'true') === 'true',
    calls: optional('FEATURE_CALLS', 'true') === 'true',
    stories: optional('FEATURE_STORIES', 'true') === 'true',
    reels: optional('FEATURE_REELS', 'true') === 'true',
    communities: optional('FEATURE_COMMUNITIES', 'true') === 'true',
    nearby: optional('FEATURE_NEARBY', 'true') === 'true',
  },
};
