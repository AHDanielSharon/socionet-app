import { useState, useEffect } from 'react';
import { api } from '@/lib/api';

export function useProfile(username: string) {
  const [profile, setProfile] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!username) return;
    setLoading(true);
    api.users.getProfile(username)
      .then(r => setProfile(r.data.user))
      .catch(err => setError(err.response?.data?.error || 'Failed to load profile'))
      .finally(() => setLoading(false));
  }, [username]);

  const toggleFollow = async () => {
    if (!profile) return;
    const wasFollowing = profile.is_following;
    setProfile((p: any) => ({ ...p, is_following: !wasFollowing, followers_count: p.followers_count + (wasFollowing ? -1 : 1) }));
    try {
      if (wasFollowing) await api.relationships.unfollow(profile.id);
      else await api.relationships.follow(profile.id);
    } catch {
      setProfile((p: any) => ({ ...p, is_following: wasFollowing, followers_count: p.followers_count + (wasFollowing ? 1 : -1) }));
    }
  };

  return { profile, loading, error, toggleFollow, setProfile };
}
