import { z } from 'zod';
export const createPostSchema = z.object({ caption: z.string().max(2200).optional(), visibility: z.enum(['public','followers','close_friends','only_me']).optional(), location_name: z.string().max(255).optional() });
export const commentSchema = z.object({ content: z.string().min(1).max(1000).trim(), parent_id: z.string().uuid().optional() });
export type CreatePostInput = z.infer<typeof createPostSchema>;
export type CommentInput = z.infer<typeof commentSchema>;
