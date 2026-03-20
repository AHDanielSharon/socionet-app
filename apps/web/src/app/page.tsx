'use client';
import { useState, useEffect, useCallback } from 'react';
import { useInView } from 'react-intersection-observer';
import { motion } from 'framer-motion';
import AppLayout from '@/components/layout/AppLayout';
import { PostCard } from '@/components/feed/PostCard';
import { StoriesRow } from '@/components/feed/StoriesRow';
import { TrendingSidebar } from '@/components/feed/TrendingSidebar';
import { SuggestedUsers } from '@/components/feed/SuggestedUsers';
import { PostSkeleton } from '@/components/feed/PostSkeleton';
import { api } from '@/lib/api';
import toast from 'react-hot-toast';

export default function HomePage() {
  const [posts, setPosts] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [nextCursor, setNextCursor] = useState<string|null>(null);
  const [hasMore, setHasMore] = useState(true);
  const { ref: bottomRef, inView } = useInView({ threshold: 0 });

  const fetch = useCallback(async (cursor?: string) => {
    try {
      if (cursor) setLoadingMore(true); else setLoading(true);
      const res = await api.feed.getHome({ limit: 15, cursor });
      const { posts: newPosts, next_cursor } = res.data;
      setPosts(p => cursor ? [...p, ...newPosts] : newPosts);
      setNextCursor(next_cursor);
      setHasMore(!!next_cursor);
    } catch { toast.error('Failed to load feed'); }
    finally { setLoading(false); setLoadingMore(false); }
  }, []);

  useEffect(() => { fetch(); }, []);
  useEffect(() => { if (inView && hasMore && !loadingMore && nextCursor) fetch(nextCursor); }, [inView, hasMore, loadingMore, nextCursor]);

  return (
    <AppLayout showRightPanel rightPanel={<div className="p-5 space-y-5"><TrendingSidebar/><SuggestedUsers/></div>}>
      <div className="max-w-[640px] mx-auto px-4 py-5">
        <div className="mb-5"><StoriesRow/></div>
        <div className="space-y-4">
          {loading
            ? Array.from({length:3}).map((_,i)=><PostSkeleton key={i}/>)
            : posts.length === 0
            ? <div className="text-center py-20"><div className="text-6xl mb-4">🌐</div><h3 className="font-display font-bold text-xl mb-2">Your feed is empty</h3><p className="text-text-3 text-sm mb-6">Follow people and join communities to see posts here</p><a href="/explore" className="btn-primary">Explore SOCIONET →</a></div>
            : posts.map((p,i)=><motion.div key={p.id} initial={{opacity:0,y:12}} animate={{opacity:1,y:0}} transition={{delay:i<5?i*0.05:0}}><PostCard post={p} onUpdate={u=>setPosts(prev=>prev.map(pp=>pp.id===p.id?{...pp,...u}:pp))} onDelete={()=>setPosts(prev=>prev.filter(pp=>pp.id!==p.id))}/></motion.div>)
          }
          <div ref={bottomRef} className="h-4"/>
          {loadingMore && <div className="flex justify-center py-4"><div className="w-6 h-6 border-2 border-accent border-t-transparent rounded-full animate-spin"/></div>}
          {!hasMore && posts.length > 0 && <div className="text-center py-6 text-text-4 text-sm">You're all caught up ✨</div>}
        </div>
      </div>
    </AppLayout>
  );
}
