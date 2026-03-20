import { useState, useEffect } from 'react';
import { api } from '@/lib/api';

export function useConversations() {
  const [conversations, setConversations] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  const load = () => {
    api.conversations.list()
      .then(r => setConversations(r.data.conversations || []))
      .catch(() => {})
      .finally(() => setLoading(false));
  };

  useEffect(() => { load(); }, []);

  const updateLastMessage = (convId: string, message: any) => {
    setConversations(prev => prev.map(c =>
      c.id === convId ? { ...c, last_message: message, last_activity_at: message.created_at } : c
    ).sort((a, b) => new Date(b.last_activity_at).getTime() - new Date(a.last_activity_at).getTime()));
  };

  const incrementUnread = (convId: string) => {
    setConversations(prev => prev.map(c =>
      c.id === convId ? { ...c, unread_count: (c.unread_count || 0) + 1 } : c
    ));
  };

  const clearUnread = (convId: string) => {
    setConversations(prev => prev.map(c => c.id === convId ? { ...c, unread_count: 0 } : c));
  };

  return { conversations, loading, updateLastMessage, incrementUnread, clearUnread, refresh: load };
}
