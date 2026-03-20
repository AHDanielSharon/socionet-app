export const formatCount = (n: number): string => {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toString();
};
export const formatDuration = (s: number): string => {
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
  return `${m}:${String(sec).padStart(2,'0')}`;
};
export const slugify = (text: string): string => text.toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/^-|-$/g,'');
export const truncate = (text: string, max: number): string => text.length > max ? `${text.slice(0,max)}…` : text;
export const parseHashtags = (text: string): string[] => [...new Set((text.match(/#([a-zA-Z0-9_]+)/g)||[]).map(t=>t.slice(1).toLowerCase()))];
export const parseMentions = (text: string): string[] => [...new Set((text.match(/@([a-zA-Z0-9_.]+)/g)||[]).map(m=>m.slice(1).toLowerCase()))];
export const isValidUrl = (url: string): boolean => { try { new URL(url); return true; } catch { return false; } };
export const randomBetween = (min: number, max: number): number => Math.floor(Math.random()*(max-min+1))+min;
