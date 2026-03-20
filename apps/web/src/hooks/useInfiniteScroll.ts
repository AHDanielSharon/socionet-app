import { useState, useEffect, useCallback, useRef } from 'react';

export function useInfiniteScroll<T>(
  fetchFn: (cursor?: string) => Promise<{ items: T[]; next_cursor: string | null }>,
  deps: any[] = []
) {
  const [items, setItems] = useState<T[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [hasMore, setHasMore] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const cursorRef = useRef<string | null>(null);
  const observerRef = useRef<IntersectionObserver>();
  const bottomRef = useRef<HTMLDivElement>(null);

  const load = useCallback(async (cursor?: string) => {
    try {
      if (cursor) setLoadingMore(true); else setLoading(true);
      const { items: newItems, next_cursor } = await fetchFn(cursor);
      setItems(prev => cursor ? [...prev, ...newItems] : newItems);
      cursorRef.current = next_cursor;
      setHasMore(!!next_cursor);
    } catch (err: any) {
      setError(err.message || 'Failed to load');
    } finally {
      setLoading(false);
      setLoadingMore(false);
    }
  }, deps);

  useEffect(() => { load(); }, [load]);

  useEffect(() => {
    if (!bottomRef.current) return;
    observerRef.current = new IntersectionObserver(([entry]) => {
      if (entry.isIntersecting && hasMore && !loadingMore && cursorRef.current) {
        load(cursorRef.current);
      }
    }, { threshold: 0.1 });
    observerRef.current.observe(bottomRef.current);
    return () => observerRef.current?.disconnect();
  }, [hasMore, loadingMore, load]);

  const refresh = () => { cursorRef.current = null; load(); };

  return { items, loading, loadingMore, hasMore, error, bottomRef, refresh, setItems };
}
