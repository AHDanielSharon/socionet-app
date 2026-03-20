import crypto from 'crypto';
export const generateToken = (bytes = 32) => crypto.randomBytes(bytes).toString('hex');
export const hashString = (s: string) => crypto.createHash('sha256').update(s).digest('hex');
export const compareHash = async (s: string, h: string) => (await import('bcryptjs')).compare(s, h);
