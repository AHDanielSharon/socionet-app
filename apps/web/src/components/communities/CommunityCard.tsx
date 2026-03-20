'use client';
import { useRouter } from 'next/navigation';
import { Lock, Shield } from 'lucide-react';
import { Avatar } from '@/components/ui/Avatar';
import { formatCount } from '@/lib/utils';
import { motion } from 'framer-motion';

export function CommunityCard({ community, index = 0, onJoin }: { community: any; index?: number; onJoin?: (id: string) => void }) {
  const router = useRouter();
  return (
    <motion.div className="card cursor-pointer overflow-hidden !p-0 hover:border-accent/50 transition-all" onClick={() => router.push(`/community/${community.slug}`)} initial={{opacity:0,y:8}} animate={{opacity:1,y:0}} transition={{delay:index*0.04}}>
      <div className="h-20 bg-gradient-hero relative overflow-hidden">{community.banner_url && <img src={community.banner_url} alt="" className="w-full h-full object-cover"/>}</div>
      <div className="p-4">
        <div className="flex items-start gap-3">
          <Avatar src={community.avatar_url} name={community.name} size={44} className="-mt-8 border-2 border-bg flex-shrink-0"/>
          <div className="flex-1 min-w-0">
            <div className="font-bold text-sm flex items-center gap-1.5">{community.name}{community.is_private&&<Lock size={12} className="text-text-4"/>}{community.is_verified&&<Shield size={12} className="text-accent"/>}</div>
            {community.description && <p className="text-xs text-text-3 mt-0.5 line-clamp-2">{community.description}</p>}
          </div>
        </div>
        <div className="flex items-center justify-between mt-3">
          <span className="text-xs text-text-3">{formatCount(community.members_count)} members</span>
          <button onClick={e=>{e.stopPropagation();onJoin?.(community.id);}} className={`btn-xs ${community.is_member?'btn-ghost':'btn-primary'}`}>{community.is_member?'✓ Joined':'Join'}</button>
        </div>
      </div>
    </motion.div>
  );
}
