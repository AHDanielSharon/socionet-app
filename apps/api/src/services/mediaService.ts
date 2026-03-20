import { db } from '@lib/db';
import { uploadImage, uploadVideo, uploadAudio, uploadFile, UploadResult } from '@lib/storage';
import { logger } from '@utils/logger';
import mime from 'mime-types';

export interface ProcessedMedia {
  media_id: string;
  type: string;
  url: string;
  thumbnail_url?: string;
  width?: number;
  height?: number;
  duration_seconds?: number;
  blurhash?: string;
  size_bytes: number;
}

export const mediaService = {
  processAndSave: async (
    file: Express.Multer.File,
    userId: string,
    options: { width?: number; height?: number; quality?: number } = {}
  ): Promise<ProcessedMedia> => {
    const mimeType = file.mimetype;
    let result: UploadResult;
    let mediaType: string;

    if (mimeType.startsWith('image/')) {
      result = await uploadImage(file.buffer, file.originalname, options);
      mediaType = 'image';
    } else if (mimeType.startsWith('video/')) {
      result = await uploadVideo(file.buffer, file.originalname);
      mediaType = 'video';
    } else if (mimeType.startsWith('audio/')) {
      result = await uploadAudio(file.buffer, file.originalname);
      mediaType = 'audio';
    } else {
      result = await uploadFile(file.buffer, file.originalname, mimeType);
      mediaType = 'document';
    }

    const saved = await db.queryOne<{ id: string }>(
      `INSERT INTO media (user_id, type, url, key, bucket, size_bytes, width, height, mime_type, blurhash, original_name)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
       RETURNING id`,
      [
        userId, mediaType, result.url, result.key, result.bucket,
        result.size, result.width || null, result.height || null,
        mimeType, result.blurhash || null, file.originalname,
      ]
    );

    return {
      media_id: saved!.id,
      type: mediaType,
      url: result.url,
      width: result.width,
      height: result.height,
      blurhash: result.blurhash,
      size_bytes: result.size,
    };
  },

  processMultiple: async (
    files: Express.Multer.File[],
    userId: string
  ): Promise<ProcessedMedia[]> => {
    return Promise.all(files.map(f => mediaService.processAndSave(f, userId)));
  },

  attachToPost: async (
    postId: string,
    media: ProcessedMedia[]
  ): Promise<void> => {
    for (let i = 0; i < media.length; i++) {
      await db.query(
        'INSERT INTO post_media (post_id, media_id, position) VALUES ($1, $2, $3)',
        [postId, media[i].media_id, i]
      );
    }
  },
};
