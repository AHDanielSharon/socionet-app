'use client';
import Image from 'next/image';
import { BadgeCheck, MapPin, Link, Calendar, UserPlus, UserCheck, MessageCircle, Share2, Settings } from 'lucide-react';
import { Avatar } from '@/components/ui/Avatar';
import { formatCount, formatFullDate } from '@/lib/utils';
import { useRouter } from 'next/navigation';
import { useAuthStore } from '@/stores/authStore';

export function ProfileHeader({ profile, onToggleFollow }: { profile: any; onToggleFollow: () => void }) {
  const { user: me } = useAuthStore();
  const router = useRouter();
  const isMe = profile.username === me?.username;
  return (
    <div>
      <div className="relative h-52 bg-gradient-hero overflow-hidden">
        {profile.banner_url && <Image src={profile.banner_url} alt="" fill className="object-cover"/>}
        <div className="absolute inset-0 bg-gradient-to-b from-transparent to-bg/60"/>
      </div>
      <div className="px-6">
        <div className="flex items-end justify-between -mt-14 mb-4">
          <div className="relative">
            <div className="w-28 h-28 rounded-full overflow-hidden border-4 border-bg bg-surface"><Avatar src={profile.avatar_url} name={profile.full_name} size={112}/></div>
            {profile.is_verified && <div className="absolute bottom-1 right-1 w-7 h-7 bg-accent rounded-full flex items-center justify-center border-2 border-bg"><BadgeCheck size={14} className="text-white"/></div>}
          </div>
          <div className="flex gap-2 mb-2">
            {isMe ? (
              <><button onClick={()=>router.push('/settings')} className="btn-ghost btn-sm"><Settings size={14}/></button><button className="btn-primary btn-sm">Edit Profile</button></>
            ) : (
              <><button className="btn-ghost btn-sm" onClick={()=>router.push('/messages')}><MessageCircle size={14}/></button><button onClick={onToggleFollow} className={profile.is_following?'btn-ghost btn-sm':'btn-primary btn-sm'}>{profile.is_following?<><UserCheck size={14}/> Following</>:<><UserPlus size={14}/> Follow</>}</button></>
            )}
          </div>
        </div>
        <h1 className="font-display font-black text-2xl flex items-center gap-2">{profile.full_name}{profile.is_verified&&<BadgeCheck size={18} className="text-accent"/>}</h1>
        <p className="text-text-3 font-mono text-sm">@{profile.username}</p>
        {profile.bio && <p className="text-text-2 text-sm mt-2 leading-relaxed max-w-lg">{profile.bio}</p>}
        <div className="flex gap-4 mt-2 text-sm text-text-3 flex-wrap">
          {profile.location && <span className="flex items-center gap-1"><MapPin size={13}/>{profile.location}</span>}
          {profile.website && <a href={profile.website} target="_blank" rel="noopener noreferrer" className="flex items-center gap-1 text-accent-2 hover:underline"><Link size={13}/>{profile.website.replace(/^https?:\/\//,'')}</a>}
          {profile.created_at && <span className="flex items-center gap-1"><Calendar size={13}/>Joined {formatFullDate(profile.created_at)}</span>}
        </div>
        <div className="flex gap-6 mt-4">
          {[{l:'Posts',v:profile.posts_count},{l:'Followers',v:profile.followers_count,a:()=>router.push(`/profile/${profile.username}/followers`)},{l:'Following',v:profile.following_count,a:()=>router.push(`/profile/${profile.username}/following`)}].map(s=>(
            <button key={s.l} onClick={s.a} className={`text-center ${s.a?'cursor-pointer':'cursor-default'}`}><div className="font-display font-black text-xl">{formatCount(s.v)}</div><div className="text-xs text-text-3">{s.l}</div></button>
          ))}
        </div>
      </div>
    </div>
  );
}
