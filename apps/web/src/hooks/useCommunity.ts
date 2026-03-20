import { useState, useEffect } from 'react';
import { api } from '@/lib/api';
export function useCommunity(slug: string) {
  const [community, setCommunity] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => { if (slug) api.communities.get(slug).then(r => setCommunity(r.data.community)).catch(() => {}).finally(() => setLoading(false)); }, [slug]);
  const join = async () => { if (!community) return; await api.communities.join(community.id); setCommunity((p: any) => ({...p, is_member: true})); };
  const leave = async () => { if (!community) return; await api.communities.leave(community.id); setCommunity((p: any) => ({...p, is_member: false})); };
  return { community, loading, join, leave };
}
