'use client';
import { useState, useEffect } from 'react';
import AppLayout from '@/components/layout/AppLayout';
import { api } from '@/lib/api';
import { formatCount } from '@/lib/utils';
import { Gem, ArrowUpRight, ArrowDownLeft, RefreshCw, TrendingUp } from 'lucide-react';
import { motion } from 'framer-motion';

export default function WalletPage() {
  const [wallet, setWallet] = useState<any>(null);
  const [txs, setTxs] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  useEffect(() => { api.wallet.get().then(r => { setWallet(r.data.wallet); setTxs(r.data.transactions); }).catch(()=>{}).finally(()=>setLoading(false)); }, []);
  const TX_ICONS: Record<string,string> = { tip:'💝', subscription:'🎵', nft_sale:'🎨', reward:'🏆', withdrawal:'↑', deposit:'↓', refund:'↩️', fee:'💸' };
  return (
    <AppLayout>
      <div className="max-w-3xl mx-auto px-4 py-6">
        <div className="flex items-center gap-3 mb-6"><Gem size={24} className="text-accent"/><h1 className="font-display font-black text-2xl">Wallet & Assets</h1></div>
        {loading ? <div className="skeleton h-48 rounded-3xl mb-6"/> : wallet && (
          <div className="rounded-3xl p-7 mb-6 relative overflow-hidden" style={{background:'linear-gradient(135deg,#1a0533,#062040,#0a0a1a)',border:'1px solid rgba(124,106,247,0.3)'}}>
            <div style={{position:'absolute',top:'-50%',right:'-20%',width:'300px',height:'300px',background:'radial-gradient(circle,rgba(124,106,247,0.15),transparent)',pointerEvents:'none'}}/>
            <p className="text-xs text-text-3 uppercase tracking-widest font-mono mb-2">Total Balance</p>
            <p className="font-display font-black text-4xl bg-gradient-to-r from-text to-accent-3 bg-clip-text text-transparent mb-1">{formatCount(wallet.balance_tokens)} <span className="text-lg">SNT</span></p>
            <p className="text-text-3 text-sm font-mono">{wallet.wallet_address ? `${wallet.wallet_address.slice(0,10)}...${wallet.wallet_address.slice(-6)}` : 'No wallet linked'}</p>
            <div className="flex gap-3 mt-6">
              {[['↑ Send','btn-neon'],['↓ Receive','btn-ghost'],['↔ Swap','btn-ghost']].map(([l,v])=><button key={l} className={`${v} btn-sm`} style={v==='btn-ghost'?{borderColor:'rgba(255,255,255,0.3)',color:'#fff'}:{}}>{l}</button>)}
            </div>
          </div>
        )}
        <h2 className="font-display font-bold text-lg mb-4">Transactions</h2>
        {txs.length===0 ? <p className="text-text-3 text-sm text-center py-8">No transactions yet</p> : (
          <div className="space-y-2">
            {txs.map((tx,i) => (
              <motion.div key={tx.id} className="card flex items-center gap-4" initial={{opacity:0,y:6}} animate={{opacity:1,y:0}} transition={{delay:i*0.04}}>
                <div className="w-10 h-10 rounded-full bg-surface-2 flex items-center justify-center text-lg flex-shrink-0">{TX_ICONS[tx.type]||'💸'}</div>
                <div className="flex-1"><p className="text-sm font-semibold capitalize">{tx.type.replace('_',' ')}</p><p className="text-xs text-text-3">{tx.from_username||tx.to_username||'System'}</p></div>
                <p className={`font-bold font-mono text-sm ${tx.amount_tokens > 0 ? 'text-green-400' : 'text-red-400'}`}>{tx.amount_tokens > 0 ? '+' : ''}{tx.amount_tokens} SNT</p>
              </motion.div>
            ))}
          </div>
        )}
      </div>
    </AppLayout>
  );
}
