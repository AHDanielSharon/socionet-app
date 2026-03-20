'use client';
import { useState, useEffect, useRef, useCallback } from 'react';
import { useSearchParams } from 'next/navigation';
import { motion, AnimatePresence } from 'framer-motion';
import { Send, Search, Plus, Phone, Video, Info, ArrowLeft, Smile, Paperclip, Mic } from 'lucide-react';
import AppLayout from '@/components/layout/AppLayout';
import { Avatar } from '@/components/ui/Avatar';
import { MessageBubble } from '@/components/messaging/MessageBubble';
import { useAuthStore } from '@/stores/authStore';
import { useConversations } from '@/hooks/useConversations';
import { useSocketStore, useConversationSocket } from '@/lib/socket';
import { api } from '@/lib/api';
import { formatTimeAgo, formatCount } from '@/lib/utils';
import toast from 'react-hot-toast';

export default function MessagesPage() {
  const sp = useSearchParams();
  const { user } = useAuthStore();
  const { conversations, loading: convsLoading, updateLastMessage, clearUnread } = useConversations();
  const [activeConvId, setActiveConvId] = useState<string|null>(sp.get('conv'));
  const [messages, setMessages] = useState<any[]>([]);
  const [msgsLoading, setMsgsLoading] = useState(false);
  const [input, setInput] = useState('');
  const [sending, setSending] = useState(false);
  const [replyTo, setReplyTo] = useState<any>(null);
  const [search, setSearch] = useState('');
  const [typing, setTyping] = useState<Record<string,boolean>>({});
  const endRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const typingTimer = useRef<NodeJS.Timeout>();
  const { socket } = useSocketStore();

  const activeConv = conversations.find(c => c.id === activeConvId);
  const filteredConvs = conversations.filter(c => {
    const name = c.other_user?.full_name || c.name || '';
    return name.toLowerCase().includes(search.toLowerCase());
  });

  useEffect(() => {
    if (!activeConvId) return;
    setMsgsLoading(true);
    api.conversations.getMessages(activeConvId, { limit: 50 })
      .then(r => setMessages(r.data.messages || []))
      .catch(() => {})
      .finally(() => setMsgsLoading(false));
    clearUnread(activeConvId);
    socket?.emit('conversation:join', activeConvId);
    socket?.emit('messages:read', { conversationId: activeConvId, messageIds: [] });
  }, [activeConvId]);

  useEffect(() => { endRef.current?.scrollIntoView({ behavior: 'smooth' }); }, [messages]);

  useEffect(() => {
    if (!socket || !activeConvId) return;
    const onMsg = (msg: any) => {
      if (msg.conversation_id !== activeConvId) return;
      setMessages(p => [...p, msg]);
      updateLastMessage(activeConvId, msg);
    };
    const onTyping = ({ conversationId, userId, username, isTyping }: any) => {
      if (conversationId !== activeConvId || userId === user?.id) return;
      setTyping(p => ({ ...p, [`${conversationId}:${userId}`]: isTyping }));
    };
    socket.on('message:new', onMsg);
    socket.on('typing:update', onTyping);
    return () => { socket.off('message:new', onMsg); socket.off('typing:update', onTyping); };
  }, [socket, activeConvId, user?.id]);

  const sendTyping = useCallback(() => {
    if (!activeConvId || !socket) return;
    socket.emit('typing:start', { conversationId: activeConvId });
    clearTimeout(typingTimer.current);
    typingTimer.current = setTimeout(() => socket.emit('typing:stop', { conversationId: activeConvId }), 2000);
  }, [activeConvId, socket]);

  const sendMessage = async () => {
    if (!input.trim() || !activeConvId || sending) return;
    const text = input.trim();
    setInput('');
    setReplyTo(null);
    setSending(true);
    const tempMsg = { id: `temp-${Date.now()}`, conversation_id: activeConvId, sender_id: user?.id, type: 'text', content: text, created_at: new Date().toISOString(), reply_to: replyTo, sender_name: user?.full_name, sender_avatar: user?.avatar_url };
    setMessages(p => [...p, tempMsg]);
    try {
      const res = await api.conversations.sendMessage(activeConvId, { content: text, type: 'text', reply_to_id: replyTo?.id });
      setMessages(p => p.map(m => m.id === tempMsg.id ? res.data.message : m));
      updateLastMessage(activeConvId, res.data.message);
      socket?.emit('typing:stop', { conversationId: activeConvId });
    } catch { toast.error('Failed to send'); setMessages(p => p.filter(m => m.id !== tempMsg.id)); }
    finally { setSending(false); }
  };

  const isAnyoneTyping = Object.entries(typing).some(([k, v]) => k.startsWith(activeConvId || '') && v);

  return (
    <AppLayout hideRightPanel>
      <div className="flex h-[calc(100vh-56px)] overflow-hidden">
        {/* Conversation list */}
        <div className={`w-80 border-r border-border flex flex-col bg-bg-2 flex-shrink-0 ${activeConvId ? 'hidden md:flex' : 'flex'}`}>
          <div className="p-4 border-b border-border">
            <div className="flex items-center justify-between mb-3">
              <h2 className="font-display font-bold text-lg">Messages</h2>
              <button className="btn-ghost btn-xs"><Plus size={14}/> New</button>
            </div>
            <div className="relative">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-text-3"/>
              <input value={search} onChange={e=>setSearch(e.target.value)} placeholder="Search conversations..." className="w-full bg-surface border border-border rounded-xl pl-9 pr-3 py-2 text-sm text-text placeholder:text-text-3 outline-none focus:border-accent/50"/>
            </div>
          </div>
          <div className="flex-1 overflow-y-auto">
            {convsLoading ? (
              Array.from({length:8}).map((_,i) => <div key={i} className="flex gap-3 p-4 border-b border-border/50"><div className="skeleton w-12 h-12 rounded-full"/><div className="flex-1 space-y-2"><div className="skeleton h-3 w-28 rounded"/><div className="skeleton h-2.5 w-40 rounded"/></div></div>)
            ) : filteredConvs.length === 0 ? (
              <div className="text-center py-12 px-4"><div className="text-4xl mb-3">💬</div><p className="text-text-3 text-sm">No conversations yet</p></div>
            ) : filteredConvs.map(conv => {
              const other = conv.other_user;
              const isActive = conv.id === activeConvId;
              return (
                <div key={conv.id} onClick={() => setActiveConvId(conv.id)} className={`flex items-center gap-3 p-4 cursor-pointer transition-all border-l-2 ${isActive ? 'bg-accent/10 border-accent' : 'border-transparent hover:bg-surface'}`}>
                  <div className="relative flex-shrink-0">
                    <Avatar src={other?.avatar_url} name={other?.full_name || conv.name} size={48}/>
                    {other?.is_online && <div className="absolute bottom-0.5 right-0.5 w-3 h-3 bg-green-400 rounded-full border-2 border-bg"/>}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between">
                      <span className={`text-sm truncate ${isActive ? 'text-accent font-semibold' : 'font-medium text-text'}`}>{other?.full_name || conv.name}</span>
                      {conv.last_activity_at && <span className="text-xs text-text-4 flex-shrink-0 ml-1">{formatTimeAgo(conv.last_activity_at)}</span>}
                    </div>
                    <div className="flex items-center justify-between mt-0.5">
                      <p className={`text-xs truncate ${conv.unread_count > 0 ? 'text-text font-medium' : 'text-text-3'}`}>{conv.last_message?.content || 'No messages yet'}</p>
                      {conv.unread_count > 0 && <span className="flex-shrink-0 ml-1 w-5 h-5 bg-accent rounded-full flex items-center justify-center text-[10px] text-white font-bold">{conv.unread_count}</span>}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Chat area */}
        {activeConvId && activeConv ? (
          <div className="flex-1 flex flex-col overflow-hidden">
            {/* Header */}
            <div className="px-4 py-3 border-b border-border flex items-center gap-3 bg-bg-2">
              <button className="md:hidden btn-ghost btn-xs" onClick={()=>setActiveConvId(null)}><ArrowLeft size={16}/></button>
              <Avatar src={activeConv.other_user?.avatar_url} name={activeConv.other_user?.full_name || activeConv.name} size={40}/>
              <div className="flex-1">
                <p className="font-semibold text-sm">{activeConv.other_user?.full_name || activeConv.name}</p>
                <p className="text-xs text-text-3">{activeConv.other_user?.is_online ? '● Online' : activeConv.other_user?.last_seen_at ? `Last seen ${formatTimeAgo(activeConv.other_user.last_seen_at)}` : '@' + (activeConv.other_user?.username || '')}</p>
              </div>
              <div className="flex gap-1">
                <button className="btn-ghost btn-xs"><Phone size={15}/></button>
                <button className="btn-ghost btn-xs"><Video size={15}/></button>
                <button className="btn-ghost btn-xs"><Info size={15}/></button>
              </div>
            </div>

            {/* Messages */}
            <div className="flex-1 overflow-y-auto p-4 space-y-2">
              {msgsLoading ? (
                Array.from({length:6}).map((_,i) => <div key={i} className={`flex gap-2 ${i%2===0?'flex-row-reverse':''}`}><div className="skeleton w-8 h-8 rounded-full"/><div className={`skeleton h-10 rounded-2xl ${i%3===0?'w-48':i%3===1?'w-36':'w-56'}`}/></div>)
              ) : messages.map((msg, i) => {
                const isMe = msg.sender_id === user?.id;
                const showAvatar = !isMe && (i === 0 || messages[i-1]?.sender_id !== msg.sender_id);
                return <MessageBubble key={msg.id} msg={msg} isMe={isMe} showAvatar={showAvatar} onReply={()=>setReplyTo(msg)} onDelete={()=>{}} onReact={()=>{}}/>;
              })}
              {isAnyoneTyping && (
                <div className="flex gap-2 items-center">
                  <Avatar src={activeConv.other_user?.avatar_url} name={activeConv.other_user?.full_name} size={28}/>
                  <div className="bg-surface-2 border border-border px-4 py-3 rounded-2xl rounded-bl-sm">
                    <div className="flex gap-1">{[0,1,2].map(i=><div key={i} className="w-1.5 h-1.5 rounded-full bg-text-3 animate-bounce" style={{animationDelay:`${i*0.15}s`}}/>)}</div>
                  </div>
                </div>
              )}
              <div ref={endRef}/>
            </div>

            {/* Reply preview */}
            <AnimatePresence>
              {replyTo && (
                <motion.div className="px-4 py-2 border-t border-border bg-surface/50 flex items-center gap-3" initial={{height:0,opacity:0}} animate={{height:'auto',opacity:1}} exit={{height:0,opacity:0}}>
                  <div className="flex-1 border-l-2 border-accent pl-3">
                    <p className="text-xs text-accent font-semibold">{replyTo.sender_name || 'You'}</p>
                    <p className="text-xs text-text-3 truncate">{replyTo.content}</p>
                  </div>
                  <button onClick={()=>setReplyTo(null)} className="text-text-3 hover:text-text"><span className="text-lg">×</span></button>
                </motion.div>
              )}
            </AnimatePresence>

            {/* Input */}
            <div className="px-4 pb-4 pt-2 border-t border-border bg-bg-2 flex gap-2 items-center">
              <button className="btn-ghost btn-xs flex-shrink-0"><Paperclip size={16}/></button>
              <div className="flex-1 relative">
                <input ref={inputRef} value={input} onChange={e=>{setInput(e.target.value);sendTyping();}} onKeyDown={e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendMessage();}}} placeholder="Message..." className="w-full bg-surface border border-border rounded-2xl px-4 py-2.5 text-sm text-text placeholder:text-text-3 outline-none focus:border-accent/50 transition-colors pr-10"/>
                <button className="absolute right-3 top-1/2 -translate-y-1/2 text-text-3"><Smile size={16}/></button>
              </div>
              {input.trim() ? (
                <button onClick={sendMessage} disabled={sending} className="w-9 h-9 rounded-full bg-gradient-accent flex items-center justify-center shadow-glow flex-shrink-0 hover:shadow-glow-lg transition-all">
                  <Send size={15} className="text-white"/>
                </button>
              ) : (
                <button className="btn-ghost btn-xs flex-shrink-0"><Mic size={16}/></button>
              )}
            </div>
          </div>
        ) : (
          <div className="flex-1 hidden md:flex items-center justify-center">
            <div className="text-center"><div className="text-7xl mb-4">💬</div><h3 className="font-display font-bold text-xl mb-2">Select a conversation</h3><p className="text-text-3 text-sm">Choose from your existing conversations or start a new one</p></div>
          </div>
        )}
      </div>
    </AppLayout>
  );
}
