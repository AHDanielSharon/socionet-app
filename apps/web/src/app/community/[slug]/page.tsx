'use client';
import { useState, useEffect } from 'react';
import { useParams } from 'next/navigation';
import Image from 'next/image';
import AppLayout from '@/components/layout/AppLayout';
import { PostCard } from '@/components/feed/PostCard';
import { Avatar } from '@/components/ui/Avatar';
import { api } from '@/lib/api';
import { formatCount } from '@/lib/utils';
import { Users, Lock, Shield } from 'lucide-react';
import toast from 'react-hot-toast';

export default function CommunityPage() {
  const { slug } = useParams();
  const [community, setCommunity] = useState<any>(null);
  const [posts, setPosts] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  useEffect(() => {
    if (!slug) return;
    Promise.all([api.communities.get(slug as string), api.communities.getPosts('', { limit: 20 })])
      .then(([c, p]) => { setCommunity(c.data.community); })
      .catch(()=>{}).finally(()=>setLoading(false));
  }, [slug]);
  const join = async () => {
    try { await api.communities.join(community.id); setCommunity((p:any)=>({...p,is_member:true})); toast.success('Joined! 🎉'); }
    catch { toast.error('Failed'); }
  };
  return (
    <AppLayout>
      <div className="max-w-3xl mx-auto">
        {loading ? <div className="skeleton h-48 w-full"/> : community && (
          <>
            <div className="h-48 bg-gradient-hero relative">{community.banner_url && <Image src={community.banner_url} alt="" fill className="object-cover"/>}</div>
            <div className="px-6 pb-6 -mt-8">
              <div className="flex items-end justify-between mb-4">
                <Avatar src={community.avatar_url} name={community.name} size={72} className="border-4 border-bg"/>
                <button onClick={community.is_member ? ()=>api.communities.leave(community.id) : join} className={community.is_member?'btn-ghost btn-sm':'btn-primary btn-sm'}>{community.is_member?'✓ Joined':'Join'}</button>
              </div>
              <h1 className="font-display font-bold text-2xl flex items-center gap-2">{community.name}{community.is_private&&<Lock size={16} className="text-text-4"/>}{community.is_verified&&<Shield size={16} className="text-accent"/>}</h1>
              {community.description && <p className="text-text-2 text-sm mt-2">{community.description}</p>}
              <div className="flex gap-4 mt-3 text-sm text-text-3"><span>{formatCount(community.members_count)} members</span><span>{formatCount(community.posts_count)} posts/week</span></div>
            </div>
          </>
        )}
        <div className="px-6 pb-6 space-y-4">
          {posts.map(p => <PostCard key={p.id} post={p}/>)}
        </div>
      </div>
    </AppLayout>
  );
}
