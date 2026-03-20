/**
 * Cloudinary Storage Adapter
 * Drop-in replacement for MinIO when deploying to Render (free tier)
 * Set USE_CLOUDINARY=true in environment variables
 */

import { v2 as cloudinary } from 'cloudinary';
import { logger } from '@utils/logger';

// Configure Cloudinary
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
  secure: true,
});

export interface UploadResult {
  url: string;
  key: string;
  bucket: string;
  size: number;
  width?: number;
  height?: number;
  duration?: number;
  blurhash?: string;
}

const uploadToCloudinary = async (
  buffer: Buffer,
  folder: string,
  options: Record<string, any> = {}
): Promise<UploadResult> => {
  return new Promise((resolve, reject) => {
    const stream = cloudinary.uploader.upload_stream(
      {
        folder: `socionet/${folder}`,
        resource_type: 'auto',
        quality: 'auto:good',
        fetch_format: 'auto',
        ...options,
      },
      (error, result) => {
        if (error) return reject(error);
        if (!result) return reject(new Error('No result from Cloudinary'));

        resolve({
          url: result.secure_url,
          key: result.public_id,
          bucket: 'cloudinary',
          size: result.bytes,
          width: result.width,
          height: result.height,
          duration: result.duration,
        });
      }
    );
    stream.end(buffer);
  });
};

export const uploadAvatar = (buffer: Buffer): Promise<UploadResult> =>
  uploadToCloudinary(buffer, 'avatars', {
    transformation: [
      { width: 400, height: 400, crop: 'fill', gravity: 'face' },
      { quality: 'auto:good', fetch_format: 'auto' },
    ],
  });

export const uploadBanner = (buffer: Buffer): Promise<UploadResult> =>
  uploadToCloudinary(buffer, 'banners', {
    transformation: [
      { width: 1500, height: 500, crop: 'fill' },
      { quality: 'auto:good' },
    ],
  });

export const uploadImage = async (
  buffer: Buffer,
  filename: string,
  options: { width?: number; height?: number; quality?: number } = {}
): Promise<UploadResult> =>
  uploadToCloudinary(buffer, 'posts', {
    transformation: [
      {
        width: options.width || 1920,
        height: options.height || 1920,
        crop: 'limit',
        quality: options.quality || 85,
        fetch_format: 'auto',
      },
    ],
  });

export const uploadVideo = (buffer: Buffer, filename: string): Promise<UploadResult> =>
  uploadToCloudinary(buffer, 'videos', {
    resource_type: 'video',
    eager: [
      { format: 'mp4', transformation: [{ quality: 'auto', width: 1280 }] },
    ],
    eager_async: true,
  });

export const uploadAudio = (buffer: Buffer, filename: string): Promise<UploadResult> =>
  uploadToCloudinary(buffer, 'audio', { resource_type: 'video' });

export const uploadFile = (buffer: Buffer, filename: string, mimeType: string): Promise<UploadResult> =>
  uploadToCloudinary(buffer, 'files', { resource_type: 'raw' });

export const deleteMedia = async (publicId: string): Promise<void> => {
  await cloudinary.uploader.destroy(publicId).catch(err =>
    logger.warn('Cloudinary delete failed', { publicId, error: String(err) })
  );
};

export const generateThumbnail = (publicId: string, width = 640, height = 360): string =>
  cloudinary.url(publicId, {
    width,
    height,
    crop: 'fill',
    quality: 'auto',
    format: 'jpg',
  });

export const initStorage = async (): Promise<void> => {
  if (!process.env.CLOUDINARY_CLOUD_NAME) {
    logger.warn('Cloudinary not configured — file uploads will fail');
    return;
  }
  logger.info('Cloudinary storage initialized', {
    cloud: process.env.CLOUDINARY_CLOUD_NAME,
  });
};
