'use client';
import { useState, useEffect } from 'react';
import AppLayout from '@/components/layout/AppLayout';
import { Avatar } from '@/components/ui/Avatar';
import { api } from '@/lib/api';
import { formatCount } from '@/lib/utils';
import { useRouter } from 'next/navigation';
import { motion } from 'framer-motion';
import { Plus, Users, Lock } from 'lucide-react';
import toast from 'react-hot-toast';

export default function CommunitiesPage() {
  const router = useRouter();
  const [mine, setMine] = useState<any[]>([]);
  const [discover, setDiscover] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [tab, setTab] = useState<'mine'|'discover'>('discover');

  useEffect(() => {
    Promise.all([api.communities.mine(), api.communities.list({ limit: 30 })])
      .then(([m, d]) => { setMine(m.data.communities || []); setDiscover(d.data.communities || []); })
      .catch(() => {}).finally(() => setLoading(false));
  }, []);

  const join = async (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    try { await api.communities.join(id); toast.success('Joined! 🎉'); setDiscover(p => p.map(c => c.id === id ? {...c, is_member: true} : c)); }
    catch { toast.error('Failed to join'); }
  };

  const communities = tab === 'mine' ? mine : discover;

  return (
    <AppLayout>
      <div className="max-w-4xl mx-auto px-4 py-6">
        <div className="flex items-center justify-between mb-6">
          <div><h1 className="font-display font-black text-2xl">Communities</h1><p className="text-text-3 text-sm">Find your people. Build your world.</p></div>
          <button className="btn-primary btn-sm"><Plus size={15}/> Create</button>
        </div>
        <div className="flex gap-1 bg-surface p-1 rounded-xl mb-6">
          {[{id:'discover',label:'Discover'},{id:'mine',label:'My Communities'}].map(t => (
            <button key={t.id} onClick={() => setTab(t.id as any)} className={`flex-1 py-2 rounded-lg text-sm font-semibold transition-all ${tab===t.id?'bg-surface-2 text-text shadow':'text-text-3 hover:text-text'}`}>{t.label}</button>
          ))}
        </div>
        {loading ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">{Array.from({length:6}).map((_,i)=><div key={i} className="skeleton h-48 rounded-2xl"/>)}</div>
        ) : communities.length === 0 ? (
          <div className="text-center py-20"><Users size={48} className="mx-auto mb-4 text-text-4"/><h3 className="font-display font-bold text-xl mb-2">{tab==='mine'?'No communities yet':'No communities found'}</h3><p className="text-text-3 text-sm">Discover and join communities</p></div>
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
            {communities.map((c, i) => (
              <motion.div key={c.id} className="card cursor-pointer overflow-hidden !p-0 hover:border-accent/50" onClick={() => router.push(`/community/${c.slug}`)} initial={{opacity:0,y:8}} animate={{opacity:1,y:0}} transition={{delay:i*0.04}}>
                <div className="h-24 bg-gradient-hero relative flex items-center justify-center">
                  {c.banner_url ? <img src={c.banner_url} className="w-full h-full object-cover" alt="" /> : <span className="text-4xl">{c.avatar_url ? '' : '🌐'}</span>}
                </div>
                <div className="p-4 pt-2">
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <div className="flex items-center gap-1.5 font-bold text-base">{c.name}{c.is_private && <Lock size={12} className="text-text-4"/>}{c.is_verified && <span className="text-accent text-xs">✓</span>}</div>
                      <p className="text-xs text-text-3 mt-0.5 line-clamp-2">{c.description}</p>
                    </div>
                  </div>
                  <div className="flex items-center justify-between mt-3">
                    <span className="text-xs text-text-3">{formatCount(c.members_count)} members</span>
                    <button onClick={e=>join(c.id,e)} className={`btn-xs ${c.is_member?'btn-ghost':'btn-primary'}`}>{c.is_member?'✓ Joined':'Join'}</button>
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
