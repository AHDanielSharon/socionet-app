'use client';
import AppLayout from '@/components/layout/AppLayout';
import { useNotifications } from '@/hooks/useNotifications';
import { Avatar } from '@/components/ui/Avatar';
import { formatTimeAgo } from '@/lib/utils';
import { useRouter } from 'next/navigation';
import { motion } from 'framer-motion';
import { BellOff, Check } from 'lucide-react';

const ICONS: Record<string, string> = { like:'❤️', comment:'💬', reply:'↩️', follow:'👤', follow_request:'🔔', follow_accepted:'✅', mention:'@', dm:'💬', live_started:'📡', story_reaction:'⭐', tip_received:'💰', achievement:'🏆', system:'🔔' };

export default function NotificationsPage() {
  const router = useRouter();
  const { notifications, loading, markAllRead } = useNotifications();
  const unread = notifications.filter(n => !n.is_read).length;
  return (
    <AppLayout>
      <div className="max-w-2xl mx-auto px-4 py-6">
        <div className="flex items-center justify-between mb-6">
          <div><h1 className="font-display font-black text-2xl">Notifications</h1>{unread > 0 && <p className="text-text-3 text-sm mt-0.5">{unread} unread</p>}</div>
          {unread > 0 && <button onClick={markAllRead} className="btn-ghost btn-sm"><Check size={14}/> Mark all read</button>}
        </div>
        {loading ? (
          <div className="space-y-3">{Array.from({length:8}).map((_,i) => <div key={i} className="flex gap-3 p-4 rounded-2xl bg-surface"><div className="skeleton w-11 h-11 rounded-full"/><div className="flex-1 space-y-2"><div className="skeleton h-3 w-3/4 rounded"/><div className="skeleton h-2.5 w-1/2 rounded"/></div></div>)}</div>
        ) : notifications.length === 0 ? (
          <div className="text-center py-20"><BellOff size={48} className="mx-auto mb-4 text-text-4"/><h3 className="font-display font-bold text-xl mb-2">All caught up!</h3><p className="text-text-3 text-sm">Notifications will appear here</p></div>
        ) : (
          <div className="space-y-1">
            {notifications.map((n, i) => (
              <motion.div key={n.id} className={`flex items-start gap-3 p-4 rounded-2xl cursor-pointer hover:bg-surface-2 transition-all ${!n.is_read ? 'bg-accent/5 border border-accent/10' : ''}`} onClick={() => n.action_url && router.push(n.action_url)} initial={{opacity:0,y:6}} animate={{opacity:1,y:0}} transition={{delay:i*0.03}}>
                <div className="relative flex-shrink-0">
                  <Avatar src={n.sender_avatar} name={n.sender_name} size={44}/>
                  {n.sender_name && <div className="absolute -bottom-1 -right-1 w-5 h-5 rounded-full bg-surface border border-border flex items-center justify-center text-xs">{ICONS[n.type] || '🔔'}</div>}
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-text-2">{n.sender_name && <strong className="text-text">{n.sender_name} </strong>}{n.body || n.title}</p>
                  <p className="text-xs text-text-4 mt-1">{formatTimeAgo(n.created_at)}</p>
                </div>
                {!n.is_read && <div className="w-2 h-2 rounded-full bg-accent flex-shrink-0 mt-2"/>}
              </motion.div>
            ))}
          </div>
        )}
      </div>
    </AppLayout>
  );
}
