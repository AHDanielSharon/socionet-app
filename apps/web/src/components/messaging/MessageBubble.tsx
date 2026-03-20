'use client';
import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Check, CheckCheck, Edit2, X } from 'lucide-react';
import { Avatar } from '@/components/ui/Avatar';
import { formatMessageTime } from '@/lib/utils';
import Image from 'next/image';

interface MessageBubbleProps { msg: any; isMe: boolean; showAvatar: boolean; onReply?: () => void; onEdit?: () => void; onDelete?: (forEveryone: boolean) => void; onReact?: (emoji: string) => void; currentUserId?: string }

export function MessageBubble({ msg, isMe, showAvatar, onReply, onEdit, onDelete, onReact }: MessageBubbleProps) {
  const [showActions, setShowActions] = useState(false);
  if (msg.is_deleted) return (
    <div className={`flex gap-2 ${isMe ? 'flex-row-reverse' : ''}`}>
      {!isMe && <div className="w-7 flex-shrink-0"/>}
      <div className={`px-4 py-2 rounded-2xl text-xs text-text-4 italic border border-border/30 ${isMe ? 'rounded-br-sm' : 'rounded-bl-sm'}`}>Message deleted</div>
    </div>
  );
  return (
    <div className={`flex gap-2 items-end group ${isMe ? 'flex-row-reverse' : ''}`} onMouseEnter={() => setShowActions(true)} onMouseLeave={() => setShowActions(false)}>
      {!isMe && <div className="w-7 flex-shrink-0 self-end">{showAvatar && <Avatar src={msg.sender_avatar} name={msg.sender_name} size={26}/>}</div>}
      <div className={`max-w-[75%] flex flex-col ${isMe ? 'items-end' : 'items-start'}`}>
        {msg.reply_to && <div className={`flex items-center gap-2 mb-1 ${isMe ? 'flex-row-reverse' : ''}`}><div className="bg-surface/60 border-l-2 border-accent rounded-r-xl px-3 py-1 text-xs text-text-3 max-w-[160px]"><p className="text-accent text-[10px] font-semibold">↩ {msg.reply_to.sender_username}</p><p className="truncate">{msg.reply_to.content}</p></div></div>}
        {msg.media && msg.media.type === 'image' && <div className="mb-1 rounded-2xl overflow-hidden max-w-[200px]"><Image src={msg.media.url} alt="" width={200} height={200} className="object-cover w-full"/></div>}
        {msg.content && <div className={`px-4 py-2.5 rounded-2xl text-sm leading-relaxed ${isMe ? 'bg-gradient-accent text-white rounded-br-sm' : 'bg-surface-2 text-text border border-border rounded-bl-sm'}`}>{msg.content}{msg.is_edited && <span className="text-[10px] opacity-60 ml-2">edited</span>}</div>}
        {msg.reactions?.length > 0 && <div className={`flex gap-1 mt-1 ${isMe ? 'flex-row-reverse' : ''}`}>{Object.entries(msg.reactions.reduce((a:any,r:any)=>({...a,[r.emoji]:(a[r.emoji]||0)+1}),{})).map(([e,c]:any)=><span key={e} className="bg-surface-2 border border-border rounded-full px-2 py-0.5 text-xs cursor-pointer">{e}{c>1?` ${c}`:''}</span>)}</div>}
        <div className={`flex items-center gap-1 mt-1 ${isMe ? 'flex-row-reverse' : ''}`}><span className="text-[10px] text-text-4">{formatMessageTime(msg.created_at)}</span>{isMe && (msg.read_by?.length ? <CheckCheck size={11} className="text-accent"/> : <Check size={11} className="text-text-4"/>)}</div>
      </div>
      <AnimatePresence>
        {showActions && <motion.div className={`flex items-center gap-1 ${isMe ? 'mr-1 flex-row-reverse' : 'ml-1'}`} initial={{opacity:0,scale:0.9}} animate={{opacity:1,scale:1}} exit={{opacity:0,scale:0.9}}>
          {['👍','❤️','😂'].map(e=><button key={e} onClick={()=>onReact?.(e)} className="w-7 h-7 rounded-full bg-surface-2 border border-border text-xs hover:scale-110 transition-all">{e}</button>)}
          {onReply && <button onClick={onReply} className="w-7 h-7 rounded-full bg-surface-2 border border-border flex items-center justify-center text-xs hover:bg-surface-3 transition-all">↩</button>}
          {isMe && onEdit && <button onClick={onEdit} className="w-7 h-7 rounded-full bg-surface-2 border border-border flex items-center justify-center hover:bg-surface-3 transition-all"><Edit2 size={11} className="text-text-3"/></button>}
          {isMe && onDelete && <button onClick={()=>onDelete(true)} className="w-7 h-7 rounded-full bg-surface-2 border border-border flex items-center justify-center hover:bg-red-500/20 transition-all"><X size={11} className="text-text-3"/></button>}
        </motion.div>}
      </AnimatePresence>
    </div>
  );
}
