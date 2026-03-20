'use client';
import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { TrendingUp } from 'lucide-react';
import { api } from '@/lib/api';
import { formatCount } from '@/lib/utils';

export function TrendingSidebar() {
  const router = useRouter();
  const [trending, setTrending] = useState<any[]>([]);
  useEffect(() => { api.search.trending().then(r => setTrending(r.data.trending || [])).catch(() => {}); }, []);
  if (!trending.length) return null;
  return (
    <div className="card">
      <div className="flex items-center gap-2 mb-3">
        <TrendingUp size={15} className="text-accent" />
        <h3 className="font-display font-bold text-sm">Trending</h3>
      </div>
      <div className="space-y-0.5">
        {trending.slice(0,8).map((t, i) => (
          <button key={t.name} onClick={() => router.push(`/hashtag/${t.name}`)} className="w-full text-left px-2 py-2 rounded-lg hover:bg-surface-2 transition-colors">
            <div className="text-[10px] text-text-4 font-mono">#{i+1} Trending</div>
            <div className="font-semibold text-sm text-accent-2">#{t.name}</div>
            <div className="text-xs text-text-3">{formatCount(t.posts_count)} posts</div>
          </button>
        ))}
      </div>
    </div>
  );
}
