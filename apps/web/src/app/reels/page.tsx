'use client';
import { useState, useEffect, useRef } from 'react';
import AppLayout from '@/components/layout/AppLayout';
import { Avatar } from '@/components/ui/Avatar';
import { api } from '@/lib/api';
import { formatCount } from '@/lib/utils';
import { Heart, MessageCircle, Share2, Repeat2, BadgeCheck } from 'lucide-react';
import toast from 'react-hot-toast';

export default function ReelsPage() {
  const [reels, setReels] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [current, setCurrent] = useState(0);

  useEffect(() => { api.feed.getReels().then(r=>setReels(r.data.reels||[])).catch(()=>{}).finally(()=>setLoading(false)); }, []);

  const reel = reels[current];

  const like = async () => {
    if (!reel) return;
    try { await api.posts.like(reel.id,'like'); setReels(p=>p.map((r,i)=>i===current?{...r,is_liked:!r.is_liked,likes_count:r.likes_count+(r.is_liked?-1:1)}:r)); }
    catch {}
  };

  return (
    <AppLayout>
      <div className="h-[calc(100vh-56px)] bg-black flex items-center justify-center overflow-hidden">
        {loading ? <div className="text-white">Loading reels...</div> : reels.length===0 ? (
          <div className="text-center text-white"><div className="text-5xl mb-4">🎬</div><p className="text-lg font-semibold mb-2">No reels yet</p><p className="text-white/60">Be the first to post a reel!</p></div>
        ) : (
          <div className="relative w-full max-w-[420px] h-full">
            <div className="w-full h-full bg-gradient-hero flex items-center justify-center">
              {reel.video_url ? <video src={reel.video_url} className="w-full h-full object-cover" autoPlay loop muted/> : <div className="text-7xl">{reel.first_media?.url ? '' : '🎵'}</div>}
            </div>
            <div className="absolute inset-0 bg-gradient-to-b from-black/30 via-transparent to-black/70"/>
            <div className="absolute bottom-20 left-4 right-16">
              <div className="flex items-center gap-2 mb-3">
                <Avatar src={reel.avatar_url} name={reel.full_name} size={36}/>
                <div><p className="text-white text-sm font-semibold flex items-center gap-1">{reel.full_name}{reel.is_verified&&<BadgeCheck size={13} className="text-accent"/>}</p><p className="text-white/70 text-xs">@{reel.username}</p></div>
                <button className="ml-2 border border-white/60 text-white text-xs px-3 py-0.5 rounded-full hover:bg-white/20">{reel.is_following?'Following':'Follow'}</button>
              </div>
              {reel.caption && <p className="text-white text-sm leading-relaxed">{reel.caption}</p>}
            </div>
            <div className="absolute right-3 bottom-20 flex flex-col gap-5 items-center">
              <button onClick={like} className="flex flex-col items-center text-white">
                <span className="text-2xl">{reel.is_liked?'❤️':'🤍'}</span>
                <span className="text-xs font-semibold mt-0.5">{formatCount(reel.likes_count)}</span>
              </button>
              <button className="flex flex-col items-center text-white"><MessageCircle size={26}/><span className="text-xs font-semibold mt-0.5">{formatCount(reel.comments_count)}</span></button>
              <button onClick={()=>{navigator.clipboard?.writeText(`${location.origin}/post/${reel.id}`);toast.success('Link copied!');}} className="flex flex-col items-center text-white"><Share2 size={26}/><span className="text-xs font-semibold mt-0.5">Share</span></button>
              <button className="flex flex-col items-center text-white"><Repeat2 size={26}/><span className="text-xs font-semibold mt-0.5">Remix</span></button>
            </div>
            <div className="absolute bottom-4 left-4 right-4 flex gap-1">
              {reels.map((_,i) => <div key={i} className={`flex-1 h-0.5 rounded-full ${i===current?'bg-white':'bg-white/30'} transition-all`}/>)}
            </div>
            <button className="absolute left-0 top-0 bottom-0 w-1/4" onClick={()=>setCurrent(c=>Math.max(0,c-1))}/>
            <button className="absolute right-0 top-0 bottom-0 w-1/4" onClick={()=>setCurrent(c=>Math.min(reels.length-1,c+1))}/>
          </div>
        )}
      </div>
    </AppLayout>
  );
}
