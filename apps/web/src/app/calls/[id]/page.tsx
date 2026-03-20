'use client';
import { useState, useEffect, useRef } from 'react';
import { useParams, useRouter } from 'next/navigation';
import { Mic, MicOff, Video, VideoOff, PhoneOff, Monitor, Users, Volume2 } from 'lucide-react';
import { useAuthStore } from '@/stores/authStore';
import { useSocketStore } from '@/lib/socket';
import { Avatar } from '@/components/ui/Avatar';
import toast from 'react-hot-toast';

export default function CallPage() {
  const { id } = useParams();
  const router = useRouter();
  const { user } = useAuthStore();
  const { socket } = useSocketStore();
  const [muted, setMuted] = useState(false);
  const [videoOff, setVideoOff] = useState(false);
  const [duration, setDuration] = useState(0);
  const [status, setStatus] = useState<'connecting'|'active'|'ended'>('connecting');
  const localRef = useRef<HTMLVideoElement>(null);
  const remoteRef = useRef<HTMLVideoElement>(null);

  useEffect(() => {
    if (status !== 'active') return;
    const int = setInterval(() => setDuration(d => d + 1), 1000);
    return () => clearInterval(int);
  }, [status]);

  useEffect(() => {
    const t = setTimeout(() => setStatus('active'), 2000);
    return () => clearTimeout(t);
  }, []);

  const endCall = () => { socket?.emit('call:end', { callId: id, targetUserId: '' }); toast('Call ended'); router.push('/messages'); };
  const fmt = (s: number) => `${String(Math.floor(s/60)).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`;

  return (
    <div className="fixed inset-0 bg-black flex flex-col items-center justify-center">
      <div className="relative w-full h-full max-w-2xl mx-auto flex flex-col items-center justify-center gap-8">
        <div className="relative w-48 h-48">
          <Avatar src={undefined} name="Caller" size={192} className="opacity-80"/>
          {status === 'connecting' && <div className="absolute inset-0 rounded-full border-4 border-accent animate-ping opacity-30"/>}
        </div>
        <div className="text-center">
          <p className="font-display font-bold text-2xl text-white mb-1">Video Call</p>
          <p className="text-white/60 text-sm">{status === 'connecting' ? 'Connecting...' : fmt(duration)}</p>
        </div>
        <div className="absolute top-4 right-4 w-32 aspect-video bg-surface-2 rounded-xl overflow-hidden border border-border/50">
          <video ref={localRef} autoPlay muted playsInline className="w-full h-full object-cover"/>
          {videoOff && <div className="absolute inset-0 bg-surface-2 flex items-center justify-center"><VideoOff size={20} className="text-text-3"/></div>}
        </div>
        <div className="flex gap-4">
          {[{icon:muted?MicOff:Mic,action:()=>{setMuted(!muted);socket?.emit('call:mute',{callId:id,isMuted:!muted});},active:!muted},{icon:videoOff?VideoOff:Video,action:()=>setVideoOff(!videoOff),active:!videoOff},{icon:Monitor,action:()=>{},active:false}].map(({icon:Icon,action,active},i)=>(
            <button key={i} onClick={action} className={`w-14 h-14 rounded-full flex items-center justify-center border transition-all ${active?'bg-white/20 border-white/40 hover:bg-white/30':'bg-white/10 border-white/20 hover:bg-white/20'}`}>
              <Icon size={22} className="text-white"/>
            </button>
          ))}
          <button onClick={endCall} className="w-14 h-14 rounded-full bg-red-500 flex items-center justify-center hover:bg-red-600 transition-all shadow-lg">
            <PhoneOff size={22} className="text-white"/>
          </button>
        </div>
      </div>
    </div>
  );
}
