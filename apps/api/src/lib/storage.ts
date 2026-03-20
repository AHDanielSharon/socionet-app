import * as Minio from 'minio';
import sharp from 'sharp';
import { v4 as uuidv4 } from 'uuid';
import path from 'path';
import { config } from '@config/index';
import { logger } from '@utils/logger';

export const minioClient = new Minio.Client({
  endPoint: config.minio.endpoint,
  port: config.minio.port,
  useSSL: config.minio.useSSL,
  accessKey: config.minio.accessKey,
  secretKey: config.minio.secretKey,
});

export interface UploadResult {
  url: string;
  key: string;
  bucket: string;
  size: number;
  width?: number;
  height?: number;
  blurhash?: string;
}

// ── Ensure buckets exist
export const initStorage = async (): Promise<void> => {
  const buckets = Object.values(config.minio.buckets);
  for (const bucket of buckets) {
    try {
      const exists = await minioClient.bucketExists(bucket);
      if (!exists) {
        await minioClient.makeBucket(bucket, 'us-east-1');
        // Public read policy
        const policy = {
          Version: '2012-10-17',
          Statement: [{
            Effect: 'Allow',
            Principal: { AWS: ['*'] },
            Action: ['s3:GetObject'],
            Resource: [`arn:aws:s3:::${bucket}/*`],
          }],
        };
        await minioClient.setBucketPolicy(bucket, JSON.stringify(policy));
        logger.info(`Created storage bucket: ${bucket}`);
      }
    } catch (err) {
      logger.error(`Failed to init bucket: ${bucket}`, { error: String(err) });
    }
  }
};

export const getPublicUrl = (bucket: string, key: string): string => {
  if (config.minio.publicUrl) return `${config.minio.publicUrl}/${bucket}/${key}`;
  const protocol = config.minio.useSSL ? 'https' : 'http';
  return `${protocol}://${config.minio.endpoint}:${config.minio.port}/${bucket}/${key}`;
};

// ── Image upload with processing
export const uploadImage = async (
  buffer: Buffer,
  originalName: string,
  options: {
    width?: number;
    height?: number;
    quality?: number;
    fit?: 'cover' | 'contain' | 'inside';
    bucket?: string;
    generateBlurhash?: boolean;
  } = {}
): Promise<UploadResult> => {
  const {
    width, height, quality = 85,
    fit = 'inside',
    bucket = config.minio.buckets.media,
    generateBlurhash = true,
  } = options;

  let sharpInst = sharp(buffer).withMetadata();

  if (width || height) {
    sharpInst = sharpInst.resize(width, height, { fit, withoutEnlargement: true });
  }

  const { data: webpData, info } = await sharpInst
    .webp({ quality, effort: 4 })
    .toBuffer({ resolveWithObject: true });

  // Generate blurhash for smooth loading
  let blurhash: string | undefined;
  if (generateBlurhash) {
    try {
      const { encode } = await import('blurhash');
      const { data: raw, info: ri } = await sharp(buffer)
        .resize(32, 32, { fit: 'inside' })
        .raw()
        .ensureAlpha()
        .toBuffer({ resolveWithObject: true });
      blurhash = encode(new Uint8ClampedArray(raw), ri.width, ri.height, 4, 3);
    } catch { /* blurhash is optional */ }
  }

  const key = `images/${uuidv4()}.webp`;

  await minioClient.putObject(bucket, key, webpData, webpData.length, {
    'Content-Type': 'image/webp',
    'Cache-Control': 'public, max-age=31536000, immutable',
    'x-original-name': path.basename(originalName),
  });

  return {
    url: getPublicUrl(bucket, key),
    key,
    bucket,
    size: webpData.length,
    width: info.width,
    height: info.height,
    blurhash,
  };
};

// ── Video upload
export const uploadVideo = async (
  buffer: Buffer,
  originalName: string,
  bucket = config.minio.buckets.videos
): Promise<UploadResult> => {
  const ext = path.extname(originalName).toLowerCase() || '.mp4';
  const key = `videos/${uuidv4()}${ext}`;

  const mimeMap: Record<string, string> = {
    '.mp4': 'video/mp4', '.webm': 'video/webm',
    '.mov': 'video/quicktime', '.avi': 'video/x-msvideo',
  };

  await minioClient.putObject(bucket, key, buffer, buffer.length, {
    'Content-Type': mimeMap[ext] || 'video/mp4',
    'Cache-Control': 'public, max-age=31536000',
  });

  return { url: getPublicUrl(bucket, key), key, bucket, size: buffer.length };
};

// ── Audio upload
export const uploadAudio = async (
  buffer: Buffer,
  originalName: string,
  bucket = config.minio.buckets.audio
): Promise<UploadResult> => {
  const ext = path.extname(originalName).toLowerCase() || '.mp3';
  const key = `audio/${uuidv4()}${ext}`;

  const mimeMap: Record<string, string> = {
    '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg',
    '.wav': 'audio/wav', '.m4a': 'audio/m4a', '.webm': 'audio/webm',
  };

  await minioClient.putObject(bucket, key, buffer, buffer.length, {
    'Content-Type': mimeMap[ext] || 'audio/mpeg',
    'Cache-Control': 'public, max-age=31536000',
  });

  return { url: getPublicUrl(bucket, key), key, bucket, size: buffer.length };
};

// ── Generic file upload
export const uploadFile = async (
  buffer: Buffer,
  originalName: string,
  mimeType: string,
  bucket = config.minio.buckets.media
): Promise<UploadResult> => {
  const ext = path.extname(originalName) || '';
  const key = `files/${uuidv4()}${ext}`;

  await minioClient.putObject(bucket, key, buffer, buffer.length, {
    'Content-Type': mimeType,
    'Content-Disposition': `attachment; filename="${encodeURIComponent(originalName)}"`,
  });

  return { url: getPublicUrl(bucket, key), key, bucket, size: buffer.length };
};

// ── Avatar upload with resize
export const uploadAvatar = async (buffer: Buffer): Promise<UploadResult> => {
  return uploadImage(buffer, 'avatar.jpg', {
    width: 400, height: 400,
    fit: 'cover', quality: 90,
    bucket: config.minio.buckets.avatars,
  });
};

// ── Banner upload with resize
export const uploadBanner = async (buffer: Buffer): Promise<UploadResult> => {
  return uploadImage(buffer, 'banner.jpg', {
    width: 1500, height: 500,
    fit: 'cover', quality: 85,
    bucket: config.minio.buckets.media,
  });
};

// ── Generate thumbnail from video buffer
export const generateVideoThumbnail = async (videoBuffer: Buffer): Promise<Buffer | null> => {
  try {
    // Use first frame extraction via ffmpeg (if available)
    // For now return null - thumbnails can be generated server-side
    return null;
  } catch { return null; }
};

// ── Delete object
export const deleteObject = async (bucket: string, key: string): Promise<void> => {
  try {
    await minioClient.removeObject(bucket, key);
  } catch (err) {
    logger.error('Failed to delete storage object', { bucket, key, error: String(err) });
  }
};

// ── Generate pre-signed URL for private access
export const getPresignedUrl = async (
  bucket: string,
  key: string,
  expirySeconds = 3600
): Promise<string> => {
  return minioClient.presignedGetObject(bucket, key, expirySeconds);
};

// ── Get object stats
export const getObjectStat = async (bucket: string, key: string) => {
  try {
    return await minioClient.statObject(bucket, key);
  } catch { return null; }
};
