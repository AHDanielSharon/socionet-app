'use client';
import { useState, useEffect } from 'react';
import AppLayout from '@/components/layout/AppLayout';
import { Avatar } from '@/components/ui/Avatar';
import { api } from '@/lib/api';
import { formatCount } from '@/lib/utils';
import { Radio, Users } from 'lucide-react';
import toast from 'react-hot-toast';

export default function LivePage() {
  const [streams, setStreams] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  useEffect(() => { api.live.list().then(r=>setStreams(r.data.streams||[])).catch(()=>{}).finally(()=>setLoading(false)); }, []);
  const goLive = async () => {
    const title = prompt('Stream title:');
    if (!title) return;
    try { const r = await api.live.start(title); toast.success('🔴 You are live!'); }
    catch { toast.error('Failed to start stream'); }
  };
  return (
    <AppLayout>
      <div className="max-w-5xl mx-auto px-4 py-6">
        <div className="flex items-center justify-between mb-6">
          <div><h1 className="font-display font-black text-2xl">Live Streams</h1><p className="text-text-3 text-sm">Real-time broadcasts happening now</p></div>
          <button onClick={goLive} className="btn-danger btn-sm"><Radio size={15}/> Go Live</button>
        </div>
        {loading ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">{Array.from({length:6}).map((_,i)=><div key={i} className="skeleton aspect-video rounded-2xl"/>)}</div>
        ) : streams.length === 0 ? (
          <div className="text-center py-20"><Radio size={48} className="mx-auto mb-4 text-text-4"/><h3 className="font-display font-bold text-xl mb-2">No live streams right now</h3><p className="text-text-3 text-sm">Be the first to go live!</p></div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            {streams.map((s:any)=>(
              <div key={s.id} className="card overflow-hidden !p-0 cursor-pointer hover:border-red-500/50 transition-all group">
                <div className="aspect-video bg-gradient-hero relative flex items-center justify-center">
                  <div className="text-5xl">📡</div>
                  <div className="absolute top-3 left-3 flex items-center gap-1.5 bg-red-500 text-white text-xs font-bold px-2 py-1 rounded-full live-pulse"><div className="w-1.5 h-1.5 rounded-full bg-white"/>LIVE</div>
                  <div className="absolute top-3 right-3 bg-black/60 text-white text-xs px-2 py-1 rounded-full flex items-center gap-1"><Users size={10}/> {formatCount(s.total_viewers||0)}</div>
                  <div className="absolute inset-0 bg-black/50 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center"><div className="w-14 h-14 bg-white/20 rounded-full flex items-center justify-center backdrop-blur-sm text-2xl">▶️</div></div>
                </div>
                <div className="p-4">
                  <div className="flex gap-3">
                    <Avatar src={s.avatar_url} name={s.full_name} size={36}/>
                    <div><p className="font-semibold text-sm line-clamp-1">{s.title}</p><p className="text-xs text-text-3">@{s.username}</p></div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </AppLayout>
  );
}
