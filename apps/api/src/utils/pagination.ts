import { Request } from 'express';
export const getPagination = (req: Request, def = 20, max = 100) => ({ limit: Math.min(parseInt(req.query.limit as string) || def, max), cursor: req.query.cursor as string | undefined, offset: parseInt(req.query.offset as string) || 0 });
export const paginatedResponse = <T>(items: T[], cursor: string | null) => ({ items, next_cursor: cursor, has_more: !!cursor });
