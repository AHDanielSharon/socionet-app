import winston from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';
import { config } from '@config/index';

const { combine, timestamp, printf, colorize, errors, json } = winston.format;

const devFormat = printf(({ level, message, timestamp, stack, ...meta }) => {
  const metaStr = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : '';
  return `${timestamp} [${level}] ${stack || message}${metaStr}`;
});

export const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: combine(
    timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    errors({ stack: true }),
    config.app.isDev ? combine(colorize(), devFormat) : json()
  ),
  transports: [
    new winston.transports.Console(),
    ...(config.app.isProd ? [
      new DailyRotateFile({
        filename: 'logs/error-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        level: 'error',
        maxSize: '20m',
        maxFiles: '14d',
      }),
      new DailyRotateFile({
        filename: 'logs/combined-%DATE%.log',
        datePattern: 'YYYY-MM-DD',
        maxSize: '20m',
        maxFiles: '7d',
      }),
    ] : []),
  ],
});

// Stream for Morgan HTTP logger
export const httpLogStream = {
  write: (message: string) => logger.http(message.trim()),
};
