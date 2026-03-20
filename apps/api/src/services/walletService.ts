import { db } from '@lib/db';
import { logger } from '@utils/logger';

export const walletService = {
  getBalance: async (userId: string) => {
    return db.queryOne('SELECT * FROM wallets WHERE user_id = $1', [userId]);
  },

  tip: async (fromUserId: string, toUserId: string, amount: number, message?: string) => {
    return db.transaction(async (client) => {
      const sender = await client.query('SELECT balance_tokens FROM wallets WHERE user_id = $1 FOR UPDATE', [fromUserId]);
      if (!sender.rows[0] || sender.rows[0].balance_tokens < amount) throw new Error('Insufficient balance');

      await client.query('UPDATE wallets SET balance_tokens = balance_tokens - $1, total_spent = total_spent + $1 WHERE user_id = $2', [amount, fromUserId]);
      await client.query('UPDATE wallets SET balance_tokens = balance_tokens + $1, total_earned = total_earned + $1 WHERE user_id = $2', [amount, toUserId]);

      const tx = await client.query(
        `INSERT INTO wallet_transactions (from_user_id, to_user_id, amount_tokens, type, message)
         VALUES ($1, $2, $3, 'tip', $4) RETURNING id`,
        [fromUserId, toUserId, amount, message || null]
      );

      return tx.rows[0];
    });
  },

  getTransactions: async (userId: string, limit = 20) => {
    return db.queryMany(
      `SELECT wt.*, fu.username AS from_username, tu.username AS to_username
       FROM wallet_transactions wt
       LEFT JOIN users fu ON fu.id = wt.from_user_id
       LEFT JOIN users tu ON tu.id = wt.to_user_id
       WHERE wt.from_user_id = $1 OR wt.to_user_id = $1
       ORDER BY wt.created_at DESC LIMIT $2`,
      [userId, limit]
    );
  },
};
