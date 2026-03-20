import { useState, useCallback } from 'react';
import { useDebounce } from './useDebounce';
import { api } from '@/lib/api';

export function useSearch() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const debouncedQuery = useDebounce(query, 300);

  const search = useCallback(async (q: string) => {
    if (!q.trim()) { setResults(null); return; }
    setLoading(true);
    try {
      const res = await api.search.query(q);
      setResults(res.data.results);
    } catch {} finally { setLoading(false); }
  }, []);

  useState(() => {
    if (debouncedQuery) search(debouncedQuery);
    else setResults(null);
  });

  return { query, setQuery, results, loading };
}
