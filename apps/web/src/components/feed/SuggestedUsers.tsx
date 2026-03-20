'use client';
import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { Users } from 'lucide-react';
import { Avatar } from '@/components/ui/Avatar';
import { api } from '@/lib/api';
import { formatCount } from '@/lib/utils';
import toast from 'react-hot-toast';

export function SuggestedUsers() {
  const router = useRouter();
  const [users, setUsers] = useState<any[]>([]);
  const [followed, setFollowed] = useState<Set<string>>(new Set());
  useEffect(() => { api.users.getSuggestions().then(r => setUsers(r.data.users || [])).catch(() => {}); }, []);
  const follow = async (uid: string, e: React.MouseEvent) => {
    e.stopPropagation();
    try { await api.relationships.follow(uid); setFollowed(p => new Set([...p, uid])); toast.success('Following!'); }
    catch { toast.error('Failed'); }
  };
  if (!users.length) return null;
  return (
    <div className="card">
      <div className="flex items-center gap-2 mb-3">
        <Users size={15} className="text-accent" />
        <h3 className="font-display font-bold text-sm">Suggested</h3>
      </div>
      <div className="space-y-3">
        {users.slice(0,5).map(u => (
          <div key={u.id} className="flex items-center gap-2.5 cursor-pointer" onClick={() => router.push(`/profile/${u.username}`)}>
            <Avatar src={u.avatar_url} name={u.full_name} size={36} />
            <div className="flex-1 min-w-0">
              <div className="text-xs font-semibold truncate flex items-center gap-1">
                {u.full_name} {u.is_verified && <span className="text-accent text-[10px]">✓</span>}
              </div>
              <div className="text-[10px] text-text-4">{formatCount(u.followers_count)} followers</div>
            </div>
            <button onClick={e => follow(u.id, e)} className={`text-xs font-semibold px-3 py-1 rounded-lg ${followed.has(u.id) ? 'text-text-4' : 'text-accent hover:bg-accent/10'} transition-all`}>
              {followed.has(u.id) ? '✓' : 'Follow'}
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
