import { useState, useEffect, useRef } from 'react';
import { useSocketStore } from '@/lib/socket';
import { useCallStore } from '@/stores/callStore';
import { useRouter } from 'next/navigation';
export function useWebRTC(callId: string) {
  const { socket } = useSocketStore();
  const peerRef = useRef<RTCPeerConnection>();
  const localStreamRef = useRef<MediaStream>();
  return { localStream: localStreamRef.current, endCall: () => { localStreamRef.current?.getTracks().forEach(t => t.stop()); } };
}
