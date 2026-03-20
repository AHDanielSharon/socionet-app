import { useState, useEffect } from 'react';
import { useSocketStore } from '@/lib/socket';

export function useIsOnline(userId: string): boolean {
  const { socket } = useSocketStore();
  const [isOnline, setIsOnline] = useState(false);

  useEffect(() => {
    if (!socket || !userId) return;
    const onOnline = (data: any) => { if (data.userId === userId) setIsOnline(true); };
    const onOffline = (data: any) => { if (data.userId === userId) setIsOnline(false); };
    socket.on('presence:online', onOnline);
    socket.on('presence:offline', onOffline);
    return () => { socket.off('presence:online', onOnline); socket.off('presence:offline', onOffline); };
  }, [socket, userId]);

  return isOnline;
}

export function useOnlineUsers(userIds: string[]): Set<string> {
  const { socket } = useSocketStore();
  const [onlineUsers, setOnlineUsers] = useState<Set<string>>(new Set());

  useEffect(() => {
    if (!socket) return;
    const onOnline = ({ userId }: any) => setOnlineUsers(p => new Set([...p, userId]));
    const onOffline = ({ userId }: any) => setOnlineUsers(p => { const n = new Set(p); n.delete(userId); return n; });
    socket.on('presence:online', onOnline);
    socket.on('presence:offline', onOffline);
    return () => { socket.off('presence:online', onOnline); socket.off('presence:offline', onOffline); };
  }, [socket]);

  return onlineUsers;
}
