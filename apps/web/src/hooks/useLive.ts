import { useState, useEffect } from 'react';
import { useSocketStore } from '@/lib/socket';
export function useLiveStream(streamId: string) {
  const { socket } = useSocketStore();
  const [viewerCount, setViewerCount] = useState(0);
  const [comments, setComments] = useState<any[]>([]);
  useEffect(() => {
    if (!socket || !streamId) return;
    socket.emit('live:join', streamId);
    const onCount = (d: any) => setViewerCount(d.count);
    const onComment = (d: any) => setComments(p => [...p.slice(-99), d]);
    socket.on('live:viewer_count', onCount);
    socket.on('live:comment', onComment);
    return () => { socket.emit('live:leave', streamId); socket.off('live:viewer_count', onCount); socket.off('live:comment', onComment); };
  }, [socket, streamId]);
  const sendComment = (content: string) => socket?.emit('live:comment', { streamId, content });
  const sendReaction = (emoji: string) => socket?.emit('live:reaction', { streamId, emoji });
  return { viewerCount, comments, sendComment, sendReaction };
}
