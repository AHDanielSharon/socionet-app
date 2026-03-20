import { useState, useEffect } from 'react';
import { api } from '@/lib/api';
import { useNotifStore } from '@/lib/socket';

export function useNotifications() {
  const [notifications, setNotifications] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const { setUnreadCount } = useNotifStore();

  const load = async () => {
    try {
      const res = await api.notifications.list({ limit: 50 });
      setNotifications(res.data.notifications || []);
      setUnreadCount(res.data.unread_count || 0);
    } catch {} finally { setLoading(false); }
  };

  useEffect(() => { load(); }, []);

  const markAllRead = async () => {
    await api.notifications.markRead();
    setNotifications(prev => prev.map(n => ({ ...n, is_read: true })));
    setUnreadCount(0);
  };

  return { notifications, loading, markAllRead, refresh: load };
}
