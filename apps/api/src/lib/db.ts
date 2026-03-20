import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';
import { config } from '@config/index';
import { logger } from '@utils/logger';

const pool = new Pool({
  connectionString: config.db.url,
  min: config.db.poolMin,
  max: config.db.poolMax,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
  ssl: config.db.ssl ? { rejectUnauthorized: false } : false,
  application_name: 'socionet_api',
});

pool.on('error', (err) => logger.error('PostgreSQL pool error', { error: err.message }));
pool.on('connect', () => logger.debug('New DB connection established'));

export interface PaginationParams {
  limit?: number;
  offset?: number;
  cursor?: string;
}

export const db = {
  // Basic query
  query: async <T extends QueryResultRow = any>(
    text: string,
    params?: any[]
  ): Promise<QueryResult<T>> => {
    const start = Date.now();
    try {
      const result = await pool.query<T>(text, params);
      const duration = Date.now() - start;
      if (duration > 1000) {
        logger.warn('Slow query detected', { duration, query: text.slice(0, 100) });
      }
      return result;
    } catch (err: any) {
      logger.error('Database query error', { error: err.message, query: text.slice(0, 100) });
      throw err;
    }
  },

  // Get a single row
  queryOne: async <T extends QueryResultRow = any>(
    text: string,
    params?: any[]
  ): Promise<T | null> => {
    const result = await db.query<T>(text, params);
    return result.rows[0] || null;
  },

  // Get multiple rows
  queryMany: async <T extends QueryResultRow = any>(
    text: string,
    params?: any[]
  ): Promise<T[]> => {
    const result = await db.query<T>(text, params);
    return result.rows;
  },

  // Get client for manual transaction control
  getClient: (): Promise<PoolClient> => pool.connect(),

  // Transaction wrapper
  transaction: async <T>(fn: (client: PoolClient) => Promise<T>): Promise<T> => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }
  },

  // Paginated query helper
  paginate: async <T extends QueryResultRow = any>(
    baseQuery: string,
    params: any[],
    { limit = 20, offset = 0 }: PaginationParams
  ): Promise<{ rows: T[]; total: number; hasMore: boolean }> => {
    const countResult = await pool.query(
      `SELECT COUNT(*) FROM (${baseQuery}) AS count_query`,
      params
    );
    const total = parseInt(countResult.rows[0].count);

    const result = await pool.query<T>(
      `${baseQuery} LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, limit + 1, offset]
    );

    const hasMore = result.rows.length > limit;
    return { rows: result.rows.slice(0, limit), total, hasMore };
  },

  // Check if table exists
  tableExists: async (tableName: string): Promise<boolean> => {
    const result = await pool.query(
      "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
      [tableName]
    );
    return result.rows[0].exists;
  },

  // Health check
  healthCheck: async (): Promise<boolean> => {
    try {
      await pool.query('SELECT 1');
      return true;
    } catch {
      return false;
    }
  },

  end: () => pool.end(),
  pool,
};

export type { PoolClient };
