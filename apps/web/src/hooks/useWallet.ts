import { useState, useEffect } from 'react';
import { api } from '@/lib/api';
export function useWallet() {
  const [wallet, setWallet] = useState<any>(null);
  const [txs, setTxs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  useEffect(() => { api.wallet.get().then(r => { setWallet(r.data.wallet); setTxs(r.data.transactions || []); }).catch(() => {}).finally(() => setLoading(false)); }, []);
  const tip = async (userId: string, amount: number) => { const r = await api.wallet.tip(userId, amount); setWallet((p: any) => ({...p, balance_tokens: p.balance_tokens - amount})); return r; };
  return { wallet, txs, loading, tip };
}
