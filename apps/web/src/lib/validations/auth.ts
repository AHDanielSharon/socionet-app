import { z } from 'zod';
export const loginSchema = z.object({ identifier: z.string().min(1), password: z.string().min(1) });
export const registerSchema = z.object({ full_name: z.string().min(2), username: z.string().min(3).max(30).regex(/^[a-zA-Z0-9_.]+$/), email: z.string().email(), password: z.string().min(8) });
export const resetPasswordSchema = z.object({ email: z.string().email(), otp: z.string().length(6), new_password: z.string().min(8) });
export type LoginInput = z.infer<typeof loginSchema>;
export type RegisterInput = z.infer<typeof registerSchema>;
