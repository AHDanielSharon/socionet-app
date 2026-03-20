'use client';
import { useState, useEffect, useRef } from 'react';
import AppLayout from '@/components/layout/AppLayout';
import { Avatar } from '@/components/ui/Avatar';
import { useAuthStore } from '@/stores/authStore';
import { api } from '@/lib/api';
import { formatTimeAgo } from '@/lib/utils';
import { Send, Zap, Trash2, Plus, Bot } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import toast from 'react-hot-toast';

const SUGGESTIONS = ['✍️ Write a caption for my photo','📊 Analyze my followers','🎨 Give me content ideas','📰 What are trending topics?','💡 How can I grow faster?','🔍 Suggest hashtags for photography'];

export default function AIPage() {
  const { user } = useAuthStore();
  const [convId, setConvId] = useState<string|null>(null);
  const [messages, setMessages] = useState<any[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [available, setAvailable] = useState(true);
  const endRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    api.ai.status?.()?.then(r => setAvailable(r.data.available)).catch(() => {});
    api.ai.createConversation().then(r => {
      setConvId(r.data.conversation.id);
      setMessages([{ id: 'welcome', role: 'assistant', content: `Hey ${user?.full_name?.split(' ')[0] || 'there'}! 👋 I'm your SOCIONET AI. I can help you write posts, analyze your audience, generate hashtags, and more. What would you like to do?`, created_at: new Date().toISOString() }]);
    }).catch(() => {});
  }, []);

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  const send = async (text: string) => {
    if (!text.trim() || !convId || loading) return;
    setInput('');
    const userMsg = { id: Date.now().toString(), role: 'user', content: text, created_at: new Date().toISOString() };
    setMessages(p => [...p, userMsg]);
    setLoading(true);
    try {
      const res = await api.ai.sendMessage(convId, text);
      setMessages(p => [...p, res.data.message]);
    } catch { toast.error('Failed to get response'); }
    finally { setLoading(false); }
  };

  return (
    <AppLayout>
      <div className="flex h-[calc(100vh-56px)]">
        {/* Main chat */}
        <div className="flex-1 flex flex-col">
          {/* Header */}
          <div className="border-b border-border px-6 py-4 flex items-center gap-3 bg-bg-2">
            <div className="w-10 h-10 rounded-full bg-gradient-accent flex items-center justify-center text-xl shadow-glow animate-pulse-glow"><Bot size={20} className="text-white"/></div>
            <div>
              <h2 className="font-display font-bold">SOCIONET AI</h2>
              <p className="text-xs text-neon">{available ? '● On-device · Private · Always available' : '⚠ Configure OPENAI_API_KEY for full features'}</p>
            </div>
            <div className="ml-auto flex gap-2">
              <button onClick={() => { setMessages([]); api.ai.createConversation().then(r => setConvId(r.data.conversation.id)); }} className="btn-ghost btn-xs"><Plus size={13}/> New chat</button>
            </div>
          </div>

          {/* Messages */}
          <div className="flex-1 overflow-y-auto p-6 space-y-4">
            {messages.map((m, i) => (
              <motion.div key={m.id} className={`flex gap-3 ${m.role==='user'?'flex-row-reverse':''}`} initial={{opacity:0,y:8}} animate={{opacity:1,y:0}} transition={{delay:i<5?i*0.05:0}}>
                {m.role==='assistant' ? (
                  <div className="w-8 h-8 rounded-full bg-gradient-accent flex items-center justify-center flex-shrink-0 shadow-glow"><Bot size={14} className="text-white"/></div>
                ) : (
                  <Avatar src={user?.avatar_url} name={user?.full_name} size={32} className="flex-shrink-0"/>
                )}
                <div className={`max-w-[75%] px-4 py-3 rounded-2xl text-sm leading-relaxed ${m.role==='assistant'?'bg-surface-2 border border-border text-text-2 rounded-tl-sm':'bg-gradient-accent text-white rounded-tr-sm'}`} style={{whiteSpace:'pre-wrap'}}>{m.content}</div>
              </motion.div>
            ))}
            {loading && (
              <div className="flex gap-3">
                <div className="w-8 h-8 rounded-full bg-gradient-accent flex items-center justify-center flex-shrink-0"><Bot size={14} className="text-white"/></div>
                <div className="bg-surface-2 border border-border px-4 py-3 rounded-2xl rounded-tl-sm">
                  <div className="flex gap-1 items-center h-5">{[0,1,2].map(i=><div key={i} className="typing-dot w-1.5 h-1.5 rounded-full bg-text-3"/>)}</div>
                </div>
              </div>
            )}
            <div ref={endRef}/>
          </div>

          {/* Suggestions */}
          {messages.length <= 1 && (
            <div className="px-6 pb-2 flex gap-2 overflow-x-auto" style={{scrollbarWidth:'none'}}>
              {SUGGESTIONS.map(s => (
                <button key={s} onClick={() => send(s)} className="flex-shrink-0 px-3 py-1.5 bg-surface border border-border rounded-full text-sm text-text-2 hover:border-accent hover:text-accent transition-all whitespace-nowrap">{s}</button>
              ))}
            </div>
          )}

          {/* Input */}
          <div className="px-6 pb-6 pt-2 border-t border-border bg-bg-2">
            <div className="flex gap-2">
              <input value={input} onChange={e=>setInput(e.target.value)} onKeyDown={e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();send(input);}}} placeholder="Ask anything — content, analytics, ideas, creativity..." className="input flex-1 rounded-2xl"/>
              <button onClick={()=>send(input)} disabled={!input.trim()||loading} className="w-10 h-10 rounded-full bg-gradient-accent flex items-center justify-center disabled:opacity-50 shadow-glow hover:shadow-glow-lg transition-all flex-shrink-0">
                <Send size={16} className="text-white"/>
              </button>
            </div>
          </div>
        </div>
      </div>
    </AppLayout>
  );
}
