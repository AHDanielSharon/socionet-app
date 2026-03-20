import multer, { FileFilterCallback } from 'multer';
import { Request } from 'express';
import { config } from '@config/index';
import { AppError } from '@middleware/errorHandler';

const MB = 1024 * 1024;

const imageFilter = (_req: Request, file: Express.Multer.File, cb: FileFilterCallback) => {
  if (file.mimetype.startsWith('image/')) cb(null, true);
  else cb(new AppError('Only image files allowed', 400, 'INVALID_FILE_TYPE'));
};

const videoFilter = (_req: Request, file: Express.Multer.File, cb: FileFilterCallback) => {
  if (file.mimetype.startsWith('video/')) cb(null, true);
  else cb(new AppError('Only video files allowed', 400, 'INVALID_FILE_TYPE'));
};

const audioFilter = (_req: Request, file: Express.Multer.File, cb: FileFilterCallback) => {
  if (file.mimetype.startsWith('audio/')) cb(null, true);
  else cb(new AppError('Only audio files allowed', 400, 'INVALID_FILE_TYPE'));
};

const mediaFilter = (_req: Request, file: Express.Multer.File, cb: FileFilterCallback) => {
  const allowed = ['image/', 'video/', 'audio/'];
  if (allowed.some(t => file.mimetype.startsWith(t))) cb(null, true);
  else cb(new AppError('Only image, video, or audio files allowed', 400, 'INVALID_FILE_TYPE'));
};

const anyFilter = (_req: Request, _file: Express.Multer.File, cb: FileFilterCallback) => {
  cb(null, true);
};

const storage = multer.memoryStorage();

export const uploadImage = multer({
  storage,
  limits: { fileSize: config.upload.maxImageMb * MB, files: 1 },
  fileFilter: imageFilter,
});

export const uploadImages = multer({
  storage,
  limits: { fileSize: config.upload.maxImageMb * MB, files: 10 },
  fileFilter: imageFilter,
});

export const uploadVideo = multer({
  storage,
  limits: { fileSize: config.upload.maxVideoMb * MB, files: 1 },
  fileFilter: videoFilter,
});

export const uploadAudio = multer({
  storage,
  limits: { fileSize: config.upload.maxAudioMb * MB, files: 1 },
  fileFilter: audioFilter,
});

export const uploadMedia = multer({
  storage,
  limits: { fileSize: config.upload.maxVideoMb * MB, files: 10 },
  fileFilter: mediaFilter,
});

export const uploadAny = multer({
  storage,
  limits: { fileSize: config.upload.maxFileMb * MB, files: 1 },
  fileFilter: anyFilter,
});

export const uploadProfileMedia = multer({
  storage,
  limits: { fileSize: config.upload.maxImageMb * MB, files: 2 },
  fileFilter: imageFilter,
}).fields([
  { name: 'avatar', maxCount: 1 },
  { name: 'banner', maxCount: 1 },
]);
