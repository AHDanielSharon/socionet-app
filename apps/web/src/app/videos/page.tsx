'use client';
import { useState, useEffect } from 'react';
import AppLayout from '@/components/layout/AppLayout';
import { Avatar } from '@/components/ui/Avatar';
import { api } from '@/lib/api';
import { formatCount, formatDuration } from '@/lib/utils';
import { Play } from 'lucide-react';
import { useRouter } from 'next/navigation';
import { motion } from 'framer-motion';

export default function VideosPage() {
  const router = useRouter();
  const [videos, setVideos] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  useEffect(() => { api.feed.getVideos().then(r=>setVideos(r.data.videos||[])).catch(()=>{}).finally(()=>setLoading(false)); }, []);
  return (
    <AppLayout>
      <div className="max-w-5xl mx-auto px-4 py-6">
        <h1 className="font-display font-black text-2xl mb-6">Videos</h1>
        {loading ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5">{Array.from({length:6}).map((_,i)=><div key={i} className="skeleton aspect-video rounded-xl"/>)}</div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5">
            {videos.map((v,i)=>(
              <motion.div key={v.id} className="cursor-pointer group" onClick={()=>router.push(`/post/${v.id}`)} initial={{opacity:0,y:8}} animate={{opacity:1,y:0}} transition={{delay:i*0.04}}>
                <div className="aspect-video bg-gradient-hero rounded-xl overflow-hidden relative mb-3">
                  {v.thumbnail_url ? <img src={v.thumbnail_url} alt="" className="w-full h-full object-cover"/> : <div className="w-full h-full flex items-center justify-center text-4xl">🎬</div>}
                  <div className="absolute inset-0 bg-black/50 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center"><div className="w-12 h-12 bg-white/20 rounded-full flex items-center justify-center backdrop-blur-sm"><Play size={22} className="text-white ml-0.5"/></div></div>
                  {v.duration_seconds && <div className="absolute bottom-2 right-2 bg-black/80 text-white text-xs px-1.5 py-0.5 rounded font-mono">{formatDuration(v.duration_seconds)}</div>}
                </div>
                <div className="flex gap-2.5">
                  <Avatar src={v.avatar_url} name={v.full_name} size={32} className="flex-shrink-0 mt-0.5"/>
                  <div>
                    <p className="text-sm font-semibold line-clamp-2 leading-snug">{v.caption || 'Untitled'}</p>
                    <p className="text-xs text-text-3 mt-1">@{v.username} · {formatCount(v.views_count)} views</p>
                  </div>
                </div>
              </motion.div>
            ))}
          </div>
        )}
      </div>
    </AppLayout>
  );
}
