import crypto from 'crypto';
export const generateOTP = (len=6): string => Math.floor(Math.pow(10,len-1)+Math.random()*9*Math.pow(10,len-1)).toString();
export const hashString = (s: string): string => crypto.createHash('sha256').update(s).digest('hex');
export const slugify = (t: string): string => t.toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'');
export const parseHashtags = (t: string): string[] => [...new Set((t.match(/#([a-zA-Z0-9_]+)/g)||[]).map(h=>h.slice(1).toLowerCase()))];
export const parseMentions = (t: string): string[] => [...new Set((t.match(/@([a-zA-Z0-9_.]+)/g)||[]).map(m=>m.slice(1).toLowerCase()))];
export const sleep = (ms: number) => new Promise(r=>setTimeout(r,ms));
