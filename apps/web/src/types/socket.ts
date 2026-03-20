export interface SocketEvents {
  'message:new': { conversation_id: string; id: string; content: string; sender_id: string; created_at: string; [key: string]: any };
  'typing:update': { conversationId: string; userId: string; username: string; isTyping: boolean };
  'notification:new': { id: string; type: string; title?: string; body?: string; [key: string]: any };
  'presence:online': { userId: string; username: string };
  'presence:offline': { userId: string; username: string };
  'call:incoming': { callId: string; callType: string; from: { userId: string; username: string } };
  'call:answered': { callId: string; answer: any; from: string };
  'call:rejected': { callId: string; by: string };
  'call:ended': { callId: string; by: string };
  'call:ice_candidate': { callId: string; candidate: RTCIceCandidateInit; from: string };
  'call:sdp_offer': { callId: string; sdp: RTCSessionDescriptionInit; from: string };
  'call:sdp_answer': { callId: string; sdp: RTCSessionDescriptionInit; from: string };
  'live:reaction': { userId: string; username: string; emoji: string; ts: number };
  'live:comment': { userId: string; username: string; content: string; ts: string };
  'live:viewer_count': { streamId: string; count: number };
  'feed:new_post': { postId: string; userId: string; username: string };
}
