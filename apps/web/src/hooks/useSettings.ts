import { useState, useEffect } from 'react';
import { api } from '@/lib/api';
export function useSettings() {
  const [settings, setSettings] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => { api.auth.me().then(r => setSettings(r.data.user)).catch(() => {}).finally(() => setLoading(false)); }, []);
  const update = async (updates: any) => { setSettings((p: any) => ({...p, ...updates})); };
  return { settings, loading, update };
}
