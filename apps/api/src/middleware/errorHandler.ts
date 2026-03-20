// errorHandler.ts
import { Request, Response, NextFunction } from 'express';
import { logger } from '@utils/logger';

export class AppError extends Error {
  constructor(
    public message: string,
    public statusCode: number = 500,
    public code?: string,
    public details?: any
  ) {
    super(message);
    this.name = 'AppError';
    Error.captureStackTrace(this, this.constructor);
  }
}

export class NotFoundError extends AppError {
  constructor(resource = 'Resource') {
    super(`${resource} not found`, 404, 'NOT_FOUND');
  }
}

export class UnauthorizedError extends AppError {
  constructor(msg = 'Unauthorized') {
    super(msg, 401, 'UNAUTHORIZED');
  }
}

export class ForbiddenError extends AppError {
  constructor(msg = 'Forbidden') {
    super(msg, 403, 'FORBIDDEN');
  }
}

export class ValidationError extends AppError {
  constructor(message: string, details?: any) {
    super(message, 400, 'VALIDATION_ERROR', details);
  }
}

export class ConflictError extends AppError {
  constructor(msg = 'Resource already exists') {
    super(msg, 409, 'CONFLICT');
  }
}

export const notFoundHandler = (req: Request, res: Response): void => {
  res.status(404).json({
    error: `Route ${req.method} ${req.path} not found`,
    code: 'ROUTE_NOT_FOUND',
  });
};

export const errorHandler = (
  err: any,
  req: Request,
  res: Response,
  _next: NextFunction
): void => {
  // Log the error
  if (err.statusCode >= 500 || !err.statusCode) {
    logger.error('Unhandled error', {
      message: err.message,
      stack: err.stack,
      path: req.path,
      method: req.method,
      body: req.body,
    });
  }

  // AppError instances
  if (err instanceof AppError) {
    res.status(err.statusCode).json({
      error: err.message,
      code: err.code,
      ...(err.details ? { details: err.details } : {}),
    });
    return;
  }

  // Postgres duplicate key
  if (err.code === '23505') {
    const match = err.detail?.match(/Key \((.+)\)=/);
    const field = match?.[1] || 'field';
    res.status(409).json({ error: `${field} already exists`, code: 'DUPLICATE' });
    return;
  }

  // Postgres foreign key violation
  if (err.code === '23503') {
    res.status(400).json({ error: 'Referenced resource not found', code: 'FOREIGN_KEY' });
    return;
  }

  // Postgres not null violation
  if (err.code === '23502') {
    res.status(400).json({ error: `${err.column} is required`, code: 'NULL_VIOLATION' });
    return;
  }

  // Multer errors
  if (err.code === 'LIMIT_FILE_SIZE') {
    res.status(413).json({ error: 'File too large', code: 'FILE_TOO_LARGE' });
    return;
  }

  if (err.code === 'LIMIT_UNEXPECTED_FILE') {
    res.status(400).json({ error: 'Unexpected file field', code: 'UNEXPECTED_FILE' });
    return;
  }

  // JWT errors
  if (err.name === 'JsonWebTokenError') {
    res.status(401).json({ error: 'Invalid token', code: 'INVALID_TOKEN' });
    return;
  }

  if (err.name === 'TokenExpiredError') {
    res.status(401).json({ error: 'Token expired', code: 'TOKEN_EXPIRED' });
    return;
  }

  // Default 500
  res.status(500).json({
    error: process.env.NODE_ENV === 'production' ? 'Internal server error' : err.message,
    code: 'INTERNAL_ERROR',
  });
};
