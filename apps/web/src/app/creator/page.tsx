'use client';
import { useState, useEffect } from 'react';
import AppLayout from '@/components/layout/AppLayout';
import { Palette, TrendingUp, Users, Eye, DollarSign, Calendar, BarChart2 } from 'lucide-react';
import { useAuthStore } from '@/stores/authStore';
import { formatCount } from '@/lib/utils';

export default function CreatorPage() {
  const { user } = useAuthStore();
  const stats = [
    { label:'Followers', value: user?.followers_count || 0, change:'+12%', color:'var(--accent)', icon: Users },
    { label:'Total Views', value: 284200, change:'+34%', color:'var(--neon)', icon: Eye },
    { label:'Earnings (SNT)', value: 84200, change:'+18%', color:'#00e5a0', icon: DollarSign },
    { label:'Engagement', value: '8.4%', change:'+2.1%', color:'#3d9eff', icon: TrendingUp, isText:true },
  ];
  const tools = [
    {icon:'📅',name:'Schedule Posts',desc:'Plan your content calendar'},
    {icon:'📊',name:'Deep Analytics',desc:'Understand your audience'},
    {icon:'💬',name:'Fan Messages',desc:'Direct access to supporters'},
    {icon:'🎁',name:'Tip Jar',desc:'Accept tips from fans'},
    {icon:'🔒',name:'Exclusive Content',desc:'For subscribers only'},
    {icon:'🎨',name:'Brand Kit',desc:'Your colors & identity'},
    {icon:'📧',name:'Newsletter',desc:'Email your followers'},
    {icon:'🏆',name:'Challenges',desc:'Engage your community'},
  ];
  return (
    <AppLayout>
      <div className="max-w-5xl mx-auto px-4 py-6">
        <div className="flex items-center gap-3 mb-6"><Palette size={22} className="text-accent"/><div><h1 className="font-display font-black text-2xl">Creator Studio</h1><p className="text-text-3 text-sm">Grow your audience · Earn from your content</p></div></div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
          {stats.map((s,i) => { const Icon = s.icon; return (
            <div key={s.label} className="card text-center">
              <div className="w-10 h-10 rounded-full mx-auto mb-3 flex items-center justify-center" style={{background:`${s.color}22`}}><Icon size={18} style={{color:s.color}}/></div>
              <p className="font-display font-black text-2xl" style={{color:s.color}}>{s.isText ? s.value : formatCount(s.value as number)}</p>
              <p className="text-xs text-text-3 mt-1">{s.label}</p>
              <p className="text-xs text-green-400 mt-1">{s.change} this month</p>
            </div>
          );})}
        </div>
        <div className="card mb-6">
          <h2 className="font-display font-bold text-lg mb-4">🛠️ Creator Tools</h2>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {tools.map(t => (
              <button key={t.name} className="p-4 rounded-xl bg-surface-2 hover:bg-surface-3 hover:border-accent border border-transparent transition-all text-left">
                <div className="text-2xl mb-2">{t.icon}</div>
                <div className="font-semibold text-sm">{t.name}</div>
                <div className="text-xs text-text-3 mt-1">{t.desc}</div>
              </button>
            ))}
          </div>
        </div>
        <div className="card">
          <h2 className="font-display font-bold text-lg mb-4">💰 Revenue Sources</h2>
          {[{s:'Subscriptions',a:42000,p:50},{s:'Tips & Gifts',a:22400,p:27},{s:'NFT Sales',a:12800,p:15},{s:'Brand Deals',a:7000,p:8}].map(r=>(
            <div key={r.s} className="mb-4">
              <div className="flex justify-between text-sm mb-1.5"><span className="text-text-2">{r.s}</span><span className="font-mono font-bold">{formatCount(r.a)} SNT</span></div>
              <div className="h-1.5 bg-surface-3 rounded-full overflow-hidden"><div className="h-full rounded-full bg-gradient-to-r from-accent to-neon" style={{width:`${r.p}%`}}/></div>
            </div>
          ))}
        </div>
      </div>
    </AppLayout>
  );
}
