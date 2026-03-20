'use client';
import { useState, useEffect } from 'react';
import { Plus, X } from 'lucide-react';
import { Avatar } from '@/components/ui/Avatar';
import { api } from '@/lib/api';
import { useAuthStore } from '@/stores/authStore';
import { formatTimeAgo } from '@/lib/utils';
import Image from 'next/image';
import { motion, AnimatePresence } from 'framer-motion';

export function StoriesRow() {
  const { user } = useAuthStore();
  const [data, setData] = useState<any>({ own_stories: [], following_stories: [] });
  const [viewer, setViewer] = useState<any>(null);
  const [storyIdx, setStoryIdx] = useState(0);
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    api.feed.getStories().then(r => setData(r.data)).catch(() => {});
  }, []);

  useEffect(() => {
    if (!viewer) return;
    setProgress(0);
    const start = Date.now();
    const int = setInterval(() => {
      const pct = Math.min(((Date.now() - start) / 5000) * 100, 100);
      setProgress(pct);
      if (pct >= 100) {
        clearInterval(int);
        const stories = viewer.stories || [];
        if (storyIdx < stories.length - 1) setStoryIdx(i => i + 1);
        else setViewer(null);
      }
    }, 50);
    return () => clearInterval(int);
  }, [viewer, storyIdx]);

  const open = (group: any) => { setViewer(group); setStoryIdx(0); };
  const close = () => setViewer(null);

  const groups = [
    ...(data.following_stories || []),
  ];

  const cur = viewer?.stories?.[storyIdx];

  return (
    <>
      <div className="flex gap-4 overflow-x-auto pb-1" style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}>
        <button className="flex flex-col items-center gap-1.5 flex-shrink-0">
          <div className="relative w-16 h-16">
            <div className="w-full h-full rounded-full overflow-hidden border-2 border-dashed border-border hover:border-accent transition-colors">
              <Avatar src={user?.avatar_url} name={user?.full_name} size={60} />
            </div>
            <div className="absolute -bottom-0.5 -right-0.5 w-5 h-5 rounded-full bg-gradient-accent flex items-center justify-center border-2 border-bg">
              <Plus size={10} className="text-white" />
            </div>
          </div>
          <span className="text-[11px] text-text-3 w-16 text-center truncate">Your story</span>
        </button>

        {groups.map((g: any) => (
          <button key={g.user_id} onClick={() => open(g)} className="flex flex-col items-center gap-1.5 flex-shrink-0">
            <div className="w-16 h-16 rounded-full p-[2.5px]" style={{ background: g.has_unseen ? 'linear-gradient(135deg,#7c6af7,#00f5d4,#ff6eb4)' : '#222230' }}>
              <div className="w-full h-full rounded-full overflow-hidden border-2 border-bg">
                <Avatar src={g.avatar_url} name={g.full_name} size={56} />
              </div>
            </div>
            <span className="text-[11px] text-text-2 w-16 text-center truncate">{g.full_name?.split(' ')[0]}</span>
          </button>
        ))}
      </div>

      <AnimatePresence>
        {viewer && (
          <motion.div className="fixed inset-0 z-[900] bg-black flex items-center justify-center" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}>
            <div className="relative w-full max-w-[400px] h-full bg-black overflow-hidden">
              <div className="absolute top-3 left-3 right-3 flex gap-1 z-20">
                {viewer.stories?.map((_: any, i: number) => (
                  <div key={i} className="flex-1 h-0.5 bg-white/30 rounded-full overflow-hidden">
                    <div className="h-full bg-white" style={{ width: i < storyIdx ? '100%' : i === storyIdx ? `${progress}%` : '0%' }} />
                  </div>
                ))}
              </div>
              <div className="absolute top-8 left-4 right-4 z-20 flex items-center gap-3">
                <Avatar src={viewer.avatar_url} name={viewer.full_name} size={36} />
                <div className="flex-1">
                  <p className="text-white text-sm font-semibold">{viewer.full_name}</p>
                  {cur && <p className="text-white/60 text-xs">{formatTimeAgo(cur.created_at)}</p>}
                </div>
                <button onClick={close}><X size={20} className="text-white" /></button>
              </div>
              {cur?.media?.url ? (
                cur.media.type === 'video'
                  ? <video src={cur.media.url} className="w-full h-full object-cover" autoPlay muted loop />
                  : <Image src={cur.media.url} alt="" fill className="object-cover" />
              ) : (
                <div className="w-full h-full bg-gradient-hero flex items-center justify-center p-8">
                  <p className="text-white text-xl text-center font-semibold">{cur?.caption}</p>
                </div>
              )}
              <div className="absolute inset-0 bg-gradient-to-b from-black/40 via-transparent to-black/60" />
              {cur?.caption && cur?.media?.url && (
                <div className="absolute bottom-20 left-4 right-4 z-20">
                  <p className="text-white text-sm font-medium">{cur.caption}</p>
                </div>
              )}
              <div className="absolute bottom-4 left-4 right-4 z-20 flex gap-2">
                <input placeholder="Reply to story..." className="flex-1 bg-white/15 border border-white/30 rounded-full px-4 py-2 text-white text-sm placeholder:text-white/60 outline-none" />
                <button className="w-10 h-10 bg-white/20 rounded-full flex items-center justify-center text-white">➤</button>
              </div>
              <button className="absolute left-0 top-0 bottom-0 w-1/3 z-10" onClick={() => storyIdx > 0 && setStoryIdx(i => i - 1)} />
              <button className="absolute right-0 top-0 bottom-0 w-1/3 z-10" onClick={() => { const s = viewer.stories || []; storyIdx < s.length - 1 ? setStoryIdx(i => i + 1) : close(); }} />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}
