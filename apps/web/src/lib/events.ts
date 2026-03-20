// Client-side event bus
type Handler = (...args: any[]) => void;
const listeners = new Map<string, Set<Handler>>();
export const events = {
  on: (e: string, h: Handler) => { if (!listeners.has(e)) listeners.set(e, new Set()); listeners.get(e)!.add(h); return () => listeners.get(e)?.delete(h); },
  emit: (e: string, ...args: any[]) => listeners.get(e)?.forEach(h => h(...args)),
  off: (e: string, h: Handler) => listeners.get(e)?.delete(h),
};
