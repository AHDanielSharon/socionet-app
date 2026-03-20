export const APP_NAME = 'SOCIONET';
export const APP_URL = process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3000';
export const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:4000/api/v1';
export const REALTIME_URL = process.env.NEXT_PUBLIC_REALTIME_URL || 'http://localhost:4001';
export const MINIO_URL = process.env.NEXT_PUBLIC_MINIO_URL || 'http://localhost:9000';
export const POST_TYPES = ['text','image','video','carousel','reel','story'] as const;
export const MAX_CAPTION_LENGTH = 2200;
export const MAX_BIO_LENGTH = 500;
export const MAX_COMMENT_LENGTH = 1000;
export const STORY_DURATION_HOURS = 24;
export const FEED_PAGE_SIZE = 20;
export const REACTIONS = ['❤️','😂','😮','😢','😡','👍'];
export const STORY_EMOJIS = ['🔥','💯','😍','👏','💪','🎉'];
export const GRADIENT_PRESETS = [
  'linear-gradient(135deg,#7c6af7,#00f5d4)',
  'linear-gradient(135deg,#ff6eb4,#7c6af7)',
  'linear-gradient(135deg,#3d9eff,#00e5a0)',
  'linear-gradient(135deg,#ff9f43,#ff6eb4)',
  'linear-gradient(135deg,#00e5a0,#3d9eff)',
];
