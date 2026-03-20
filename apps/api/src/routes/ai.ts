import { Router, Response } from 'express';
import { authenticate, AuthRequest } from '@middleware/auth';
import { validate } from '@middleware/validate';
import { aiMessageSchema, aiQuickSchema } from '@validators/schemas';
import { aiService } from '@services/aiService';
import { db } from '@lib/db';
import { NotFoundError } from '@middleware/errorHandler';

const router = Router();

router.get('/status', authenticate, (_req, res) => {
  res.json({ available: aiService.isAvailable() });
});

router.get('/conversations', authenticate, async (req: AuthRequest, res: Response) => {
  const convs = await db.queryMany(
    `SELECT c.*, (SELECT content FROM ai_messages WHERE conversation_id = c.id ORDER BY created_at DESC LIMIT 1) AS last_message
     FROM ai_conversations c WHERE c.user_id = $1 ORDER BY c.updated_at DESC LIMIT 50`,
    [req.user!.id]
  );
  res.json({ conversations: convs });
});

router.post('/conversations', authenticate, async (req: AuthRequest, res: Response) => {
  const id = await aiService.createConversation(req.user!.id);
  const conv = await db.queryOne('SELECT * FROM ai_conversations WHERE id = $1', [id]);
  res.status(201).json({ conversation: conv });
});

router.delete('/conversations/:id', authenticate, async (req: AuthRequest, res: Response) => {
  await db.query('DELETE FROM ai_conversations WHERE id = $1 AND user_id = $2', [req.params.id, req.user!.id]);
  res.json({ deleted: true });
});

router.get('/conversations/:id/messages', authenticate, async (req: AuthRequest, res: Response) => {
  const conv = await db.queryOne('SELECT * FROM ai_conversations WHERE id = $1 AND user_id = $2', [req.params.id, req.user!.id]);
  if (!conv) throw new NotFoundError('Conversation');
  const messages = await db.queryMany('SELECT * FROM ai_messages WHERE conversation_id = $1 ORDER BY created_at ASC', [req.params.id]);
  res.json({ conversation: conv, messages });
});

router.post('/conversations/:id/messages', authenticate, validate(aiMessageSchema), async (req: AuthRequest, res: Response) => {
  const conv = await db.queryOne('SELECT * FROM ai_conversations WHERE id = $1 AND user_id = $2', [req.params.id, req.user!.id]);
  if (!conv) throw new NotFoundError('Conversation');
  const response = await aiService.chat(req.params.id, req.body.content, req.user!.id);
  const msg = await db.queryOne('SELECT * FROM ai_messages WHERE conversation_id = $1 AND role = $2 ORDER BY created_at DESC LIMIT 1', [req.params.id, 'assistant']);
  res.json({ message: msg, response });
});

router.post('/quick', authenticate, validate(aiQuickSchema), async (req: AuthRequest, res: Response) => {
  const response = await aiService.quickTask(req.body.task, req.body.context);
  res.json({ response });
});

export default router;
