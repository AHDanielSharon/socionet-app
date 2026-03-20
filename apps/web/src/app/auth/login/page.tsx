'use client';
import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { motion } from 'framer-motion';
import { Eye, EyeOff, AtSign, Lock, ArrowRight, Shield, Globe, Zap } from 'lucide-react';
import { useAuthStore } from '@/stores/authStore';
import { api } from '@/lib/api';
import toast from 'react-hot-toast';

export default function LoginPage() {
  const router = useRouter();
  const { setAuth } = useAuthStore();
  const [mode, setMode] = useState('login');
  const [showPass, setShowPass] = useState(false);
  const [loading, setLoading] = useState(false);
  const [form, setForm] = useState({ identifier:'', password:'', full_name:'', username:'', email:'' });
  const set = (k: string, v: string) => setForm(p=>({...p,[k]:v}));

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    try {
      if (mode==='login') {
        const res = await api.auth.login({ identifier: form.identifier, password: form.password });
        setAuth(res.data.user, res.data.tokens.access_token, res.data.tokens.refresh_token);
        toast.success(`Welcome back, ${res.data.user.full_name}!`);
        router.push('/');
      } else {
        const res = await api.auth.register({ full_name: form.full_name, username: form.username, email: form.email, password: form.password });
        setAuth(res.data.user, res.data.tokens.access_token, res.data.tokens.refresh_token);
        toast.success('Welcome to SOCIONET! 🚀');
        router.push('/');
      }
    } catch (err: any) { toast.error(err.response?.data?.error || 'Failed'); }
    finally { setLoading(false); }
  };

  return (
    <div className="min-h-screen bg-bg flex items-center justify-center p-4 relative overflow-hidden">
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_at_30%_20%,rgba(124,106,247,0.15),transparent_50%)]"/>
      <div className="relative z-10 w-full max-w-[420px]">
        <motion.div className="text-center mb-8" initial={{opacity:0,y:-20}} animate={{opacity:1,y:0}}>
          <div className="w-16 h-16 rounded-2xl bg-gradient-accent mx-auto mb-4 flex items-center justify-center text-white font-display font-black text-3xl shadow-glow-lg">S</div>
          <h1 className="font-display font-black text-3xl bg-gradient-to-r from-text to-accent-3 bg-clip-text text-transparent">SOCIONET</h1>
          <p className="text-text-3 text-sm mt-1">Private · Decentralized · Yours</p>
        </motion.div>
        <motion.div className="bg-surface/90 backdrop-blur-xl border border-accent/30 rounded-3xl overflow-hidden" initial={{opacity:0,scale:0.95}} animate={{opacity:1,scale:1}}>
          <div className="flex border-b border-border">
            {['login','register'].map(m=><button key={m} onClick={()=>setMode(m)} className={`flex-1 py-4 text-sm font-semibold transition-all ${mode===m?'text-accent border-b-2 border-accent bg-accent/5':'text-text-3'}`}>{m==='login'?'Sign In':'Create Account'}</button>)}
          </div>
          <form onSubmit={submit} className="p-7 space-y-4">
            {mode==='register' && <>
              <div><label className="block text-xs font-semibold text-text-2 mb-1.5">Full Name</label><input value={form.full_name} onChange={e=>set('full_name',e.target.value)} className="input" placeholder="Alex Chen" required/></div>
              <div><label className="block text-xs font-semibold text-text-2 mb-1.5">Username</label><div className="relative"><span className="absolute left-3 top-1/2 -translate-y-1/2 text-text-3">@</span><input value={form.username} onChange={e=>set('username',e.target.value)} className="input pl-7" placeholder="alexchen" required/></div></div>
              <div><label className="block text-xs font-semibold text-text-2 mb-1.5">Email</label><input type="email" value={form.email} onChange={e=>set('email',e.target.value)} className="input" placeholder="alex@example.com" required/></div>
            </>}
            {mode==='login' && <div><label className="block text-xs font-semibold text-text-2 mb-1.5">Email or Username</label><div className="relative"><AtSign size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-text-3"/><input value={form.identifier} onChange={e=>set('identifier',e.target.value)} className="input pl-9" placeholder="your@email.com" required autoComplete="username"/></div></div>}
            <div><label className="block text-xs font-semibold text-text-2 mb-1.5">Password</label><div className="relative"><Lock size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-text-3"/><input type={showPass?'text':'password'} value={form.password} onChange={e=>set('password',e.target.value)} className="input pl-9 pr-10" placeholder="••••••••" required minLength={8}/><button type="button" onClick={()=>setShowPass(!showPass)} className="absolute right-3 top-1/2 -translate-y-1/2 text-text-3">{showPass?<EyeOff size={15}/>:<Eye size={15}/>}</button></div></div>
            <button type="submit" disabled={loading} className="w-full btn-primary py-3 text-base justify-center disabled:opacity-60">{loading?'Loading...':mode==='login'?(<>Sign In <ArrowRight size={16}/>):'Create Account →'}</button>
          </form>
        </motion.div>
        <div className="mt-6 flex justify-center gap-6">
          {[[Shield,'E2E encrypted'],[Globe,'Decentralized'],[Zap,'Zero data sold']].map(([I,l]:[any,string])=><div key={l} className="flex items-center gap-1.5 text-xs text-text-4"><I size={12} className="text-neon"/><span>{l}</span></div>)}
        </div>
      </div>
    </div>
  );
}
