import { useState, useCallback } from 'react';
import { api } from '@/lib/api';
import toast from 'react-hot-toast';

export function usePostActions(postId: string, initialState: { liked: boolean; saved: boolean; likes_count: number }) {
  const [liked, setLiked] = useState(initialState.liked);
  const [saved, setSaved] = useState(initialState.saved);
  const [likesCount, setLikesCount] = useState(initialState.likes_count);

  const toggleLike = useCallback(async () => {
    const prev = liked;
    setLiked(!prev);
    setLikesCount(c => c + (prev ? -1 : 1));
    try {
      await api.posts.like(postId, prev ? 'unlike' : 'like');
    } catch {
      setLiked(prev);
      setLikesCount(c => c + (prev ? 1 : -1));
    }
  }, [liked, postId]);

  const toggleSave = useCallback(async () => {
    const prev = saved;
    setSaved(!prev);
    try {
      await api.posts.save(postId);
      toast.success(prev ? 'Removed from saved' : '🔖 Saved');
    } catch {
      setSaved(prev);
    }
  }, [saved, postId]);

  const share = useCallback(async () => {
    try {
      if (navigator.share) {
        await navigator.share({ url: `${window.location.origin}/post/${postId}` });
      } else {
        await navigator.clipboard.writeText(`${window.location.origin}/post/${postId}`);
        toast.success('Link copied! 🔗');
      }
    } catch {}
  }, [postId]);

  return { liked, saved, likesCount, toggleLike, toggleSave, share };
}
