'use client';
import { useState } from 'react';
import AppLayout from '@/components/layout/AppLayout';
import { useAuthStore } from '@/stores/authStore';
import { Settings, Lock, Bell, Palette, Globe, Shield, CreditCard, Code, Info, ChevronRight } from 'lucide-react';

const SECTIONS = [
  { id:'account', icon:Settings, label:'Account & Identity', desc:'Username, email, phone' },
  { id:'privacy', icon:Lock, label:'Privacy & Safety', desc:'Who can see and contact you' },
  { id:'notifications', icon:Bell, label:'Notifications', desc:'Control what alerts you get' },
  { id:'appearance', icon:Palette, label:'Appearance', desc:'Theme, colors, fonts' },
  { id:'language', icon:Globe, label:'Language & Region', desc:'Language, timezone, formats' },
  { id:'security', icon:Shield, label:'Security', desc:'Password, 2FA, sessions' },
  { id:'subscription', icon:CreditCard, label:'Subscription', desc:'Plans, billing, perks' },
  { id:'developer', icon:Code, label:'Developer', desc:'API keys, webhooks' },
  { id:'about', icon:Info, label:'About SOCIONET', desc:'Version, terms, privacy policy' },
];

export default function SettingsPage() {
  const { user, logout } = useAuthStore();
  const [active, setActive] = useState('privacy');
  const [toggles, setToggles] = useState<Record<string,boolean>>({
    is_private:false, show_activity_status:true, show_read_receipts:true, two_factor_enabled:false,
    push_enabled:true, likes:true, comments:true, follows:true, messages:true
  });
  const toggle = (k: string) => setToggles(p => ({...p, [k]:!p[k]}));

  return (
    <AppLayout>
      <div className="flex h-[calc(100vh-56px)] overflow-hidden">
        {/* Nav */}
        <div className="w-64 border-r border-border overflow-y-auto bg-bg-2 flex-shrink-0">
          <div className="p-4 border-b border-border"><h1 className="font-display font-bold text-lg">Settings</h1></div>
          <div className="p-2">
            {SECTIONS.map(s => { const Icon = s.icon; return (
              <button key={s.id} onClick={() => setActive(s.id)} className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-xl text-left transition-all ${active===s.id?'bg-accent/10 text-accent border-l-2 border-accent':'text-text-2 hover:bg-surface hover:text-text'}`}>
                <Icon size={16} className="flex-shrink-0"/>
                <div className="flex-1 min-w-0"><p className="text-sm font-medium">{s.label}</p></div>
                {active===s.id && <ChevronRight size={14}/>}
              </button>
            );})}
          </div>
          <div className="p-4 border-t border-border"><button onClick={() => logout()} className="w-full text-left text-sm text-red-400 hover:text-red-300 transition-colors py-2">← Sign out</button></div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-8">
          <h2 className="font-display font-bold text-xl mb-1">{SECTIONS.find(s=>s.id===active)?.label}</h2>
          <p className="text-text-3 text-sm mb-6">{SECTIONS.find(s=>s.id===active)?.desc}</p>

          {active==='privacy' && (
            <div className="space-y-0 max-w-lg">
              {[
                {key:'is_private',label:'Private Account',desc:'Only approved followers can see your posts'},
                {key:'show_activity_status',label:'Activity Status',desc:'Show when you\'re active to followers'},
                {key:'show_read_receipts',label:'Read Receipts',desc:'Let others know when you\'ve read messages'},
              ].map(s => (
                <div key={s.key} className="flex items-center justify-between py-4 border-b border-border">
                  <div><p className="text-sm font-medium">{s.label}</p><p className="text-xs text-text-3 mt-0.5">{s.desc}</p></div>
                  <button onClick={()=>toggle(s.key)} className={`w-11 h-6 rounded-full transition-colors relative flex-shrink-0 ${toggles[s.key]?'bg-accent':'bg-surface-3'}`}>
                    <span className={`absolute top-0.5 w-5 h-5 bg-white rounded-full shadow transition-transform ${toggles[s.key]?'translate-x-5':'translate-x-0.5'}`}/>
                  </button>
                </div>
              ))}
            </div>
          )}

          {active==='notifications' && (
            <div className="max-w-lg">
              {[{key:'likes',l:'Likes'},{key:'comments',l:'Comments'},{key:'follows',l:'New Followers'},{key:'messages',l:'Messages'},{key:'push_enabled',l:'Push Notifications'}].map(s => (
                <div key={s.key} className="flex items-center justify-between py-4 border-b border-border">
                  <p className="text-sm font-medium">{s.l}</p>
                  <button onClick={()=>toggle(s.key)} className={`w-11 h-6 rounded-full transition-colors relative ${toggles[s.key]?'bg-accent':'bg-surface-3'}`}>
                    <span className={`absolute top-0.5 w-5 h-5 bg-white rounded-full shadow transition-transform ${toggles[s.key]?'translate-x-5':'translate-x-0.5'}`}/>
                  </button>
                </div>
              ))}
            </div>
          )}

          {active==='appearance' && (
            <div className="max-w-lg">
              <div className="py-4 border-b border-border">
                <p className="text-sm font-medium mb-3">Theme</p>
                <div className="flex gap-3">{[['#060608','Dark','active'],['#f5f5f5','Light',''],['#0a0f1a','Midnight','']].map(([c,l,a])=>(
                  <button key={l} className={`flex flex-col items-center gap-2`}>
                    <div className="w-16 h-10 rounded-lg border-2 transition-all" style={{background:c,borderColor:a?'var(--accent)':'var(--border)'}}/>
                    <span className="text-xs text-text-3">{l}</span>
                  </button>
                ))}</div>
              </div>
              <div className="py-4 border-b border-border">
                <p className="text-sm font-medium mb-3">Accent Color</p>
                <div className="flex gap-3">{['#7c6af7','#00f5d4','#ff6eb4','#3d9eff','#00e5a0','#ffd700'].map(c=>(
                  <button key={c} className="w-8 h-8 rounded-full border-2 border-transparent hover:scale-110 transition-all" style={{background:c}}/>
                ))}</div>
              </div>
            </div>
          )}

          {active==='security' && (
            <div className="max-w-lg">
              {[{key:'two_factor_enabled',label:'Two-Factor Authentication',desc:'Add an extra layer of security'}].map(s=>(
                <div key={s.key} className="flex items-center justify-between py-4 border-b border-border">
                  <div><p className="text-sm font-medium">{s.label}</p><p className="text-xs text-text-3 mt-0.5">{s.desc}</p></div>
                  <button onClick={()=>toggle(s.key)} className={`w-11 h-6 rounded-full transition-colors relative ${toggles[s.key]?'bg-accent':'bg-surface-3'}`}>
                    <span className={`absolute top-0.5 w-5 h-5 bg-white rounded-full shadow transition-transform ${toggles[s.key]?'translate-x-5':'translate-x-0.5'}`}/>
                  </button>
                </div>
              ))}
              <div className="py-4 border-b border-border"><button className="btn-danger btn-sm">Change Password</button></div>
              <div className="py-4"><button className="btn-danger btn-sm">Sign Out All Devices</button></div>
            </div>
          )}
        </div>
      </div>
    </AppLayout>
  );
}
