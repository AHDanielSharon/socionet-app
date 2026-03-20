import { useState, useCallback } from 'react';
import { api } from '@/lib/api';
export function useFeed() {
  const [posts, setPosts] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [cursor, setCursor] = useState<string|null>(null);
  const load = useCallback(async (c?: string) => {
    try { setLoading(true); const r = await api.feed.getHome({ limit: 20, cursor: c }); setPosts(p => c ? [...p, ...r.data.posts] : r.data.posts); setCursor(r.data.next_cursor); } finally { setLoading(false); }
  }, []);
  return { posts, loading, cursor, load, updatePost: (id: string, u: any) => setPosts(p => p.map(pp => pp.id === id ? {...pp,...u} : pp)), removePost: (id: string) => setPosts(p => p.filter(pp => pp.id !== id)) };
}
