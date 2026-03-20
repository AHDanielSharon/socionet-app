import 'dotenv/config';
import { createServer } from 'http';
import { Server, Socket } from 'socket.io';
import { createClient } from 'redis';
import jwt from 'jsonwebtoken';

const PORT = process.env.REALTIME_PORT || 4001;
const JWT_SECRET = process.env.JWT_SECRET!;
const REDIS_URL = process.env.REDIS_URL!;
const APP_URL = process.env.APP_URL || 'http://localhost:3000';

// ── Redis clients
const publisher = createClient({ url: REDIS_URL });
const subscriber = createClient({ url: REDIS_URL });

publisher.connect().catch(console.error);
subscriber.connect().catch(console.error);

// ── HTTP + Socket.io
const httpServer = createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      connections: io.engine.clientsCount,
      timestamp: new Date().toISOString(),
    }));
  }
});

const io = new Server(httpServer, {
  cors: {
    origin: [APP_URL, 'http://localhost:3000', 'http://localhost:3001'],
    credentials: true,
    methods: ['GET', 'POST'],
  },
  transports: ['websocket', 'polling'],
  pingTimeout: 30000,
  pingInterval: 25000,
  maxHttpBufferSize: 5e6,
});

// ── State tracking
const userSockets = new Map<string, Set<string>>();  // userId -> socketIds
const socketUser = new Map<string, string>();          // socketId -> userId

// ── Auth middleware
io.use(async (socket: Socket, next) => {
  try {
    const token = socket.handshake.auth?.token ||
      socket.handshake.headers.authorization?.replace('Bearer ', '');
    if (!token) return next(new Error('Authentication required'));
    const decoded = jwt.verify(token, JWT_SECRET) as any;
    socket.data.userId = decoded.sub;
    socket.data.username = decoded.username;
    next();
  } catch (err) {
    next(new Error('Invalid token'));
  }
});

// ── Connection
io.on('connection', async (socket: Socket) => {
  const userId: string = socket.data.userId;
  const username: string = socket.data.username;

  console.log(`[WS] ${username} connected (${socket.id})`);

  // Register
  if (!userSockets.has(userId)) userSockets.set(userId, new Set());
  userSockets.get(userId)!.add(socket.id);
  socketUser.set(socket.id, userId);

  // Personal room
  socket.join(`user:${userId}`);

  // Mark online
  await publisher.setEx(`socionet:presence:${userId}`, 300, 'online');
  await publisher.publish('socionet:presence', JSON.stringify({ userId, status: 'online' }));
  socket.broadcast.emit('presence:online', { userId, username });

  // ── Conversation rooms
  socket.on('conversations:join', (ids: string[]) => {
    ids.forEach(id => socket.join(`conv:${id}`));
  });

  socket.on('conversation:join', (id: string) => socket.join(`conv:${id}`));
  socket.on('conversation:leave', (id: string) => socket.leave(`conv:${id}`));

  // ── Typing
  socket.on('typing:start', async ({ conversationId }: { conversationId: string }) => {
    await publisher.setEx(`socionet:typing:${conversationId}:${userId}`, 8, '1');
    socket.to(`conv:${conversationId}`).emit('typing:update', { conversationId, userId, username, isTyping: true });
  });

  socket.on('typing:stop', async ({ conversationId }: { conversationId: string }) => {
    await publisher.del(`socionet:typing:${conversationId}:${userId}`);
    socket.to(`conv:${conversationId}`).emit('typing:update', { conversationId, userId, username, isTyping: false });
  });

  // ── Read receipts
  socket.on('messages:read', ({ conversationId, messageIds }: any) => {
    socket.to(`conv:${conversationId}`).emit('messages:read_ack', {
      conversationId, userId, messageIds, read_at: new Date().toISOString(),
    });
  });

  // ── Message delivery ack
  socket.on('message:ack', ({ messageId, conversationId }: any) => {
    socket.to(`conv:${conversationId}`).emit('message:delivered', { messageId, userId });
  });

  // ── Presence heartbeat
  socket.on('presence:ping', async () => {
    await publisher.expire(`socionet:presence:${userId}`, 300);
    socket.emit('presence:pong', { timestamp: Date.now() });
  });

  // ── Live stream
  socket.on('live:join', (streamId: string) => {
    socket.join(`live:${streamId}`);
    socket.to(`live:${streamId}`).emit('live:viewer_joined', { userId, username });
    io.to(`live:${streamId}`).emit('live:viewer_count', {
      streamId,
      count: io.sockets.adapter.rooms.get(`live:${streamId}`)?.size || 0,
    });
  });

  socket.on('live:leave', (streamId: string) => {
    socket.leave(`live:${streamId}`);
    socket.to(`live:${streamId}`).emit('live:viewer_left', { userId });
  });

  socket.on('live:reaction', ({ streamId, emoji }: any) => {
    io.to(`live:${streamId}`).emit('live:reaction', { userId, username, emoji, ts: Date.now() });
  });

  socket.on('live:comment', ({ streamId, content }: any) => {
    if (!content || content.length > 500) return;
    io.to(`live:${streamId}`).emit('live:comment', {
      userId, username, content: content.slice(0, 500), ts: new Date().toISOString(),
    });
  });

  // ── WebRTC Call Signaling
  socket.on('call:initiate', ({ targetUserId, callType, callId }: any) => {
    const targetSockets = userSockets.get(targetUserId);
    if (targetSockets?.size) {
      io.to(`user:${targetUserId}`).emit('call:incoming', {
        callId, callType, from: { userId, username },
      });
    } else {
      socket.emit('call:unavailable', { targetUserId, reason: 'User is offline' });
    }
  });

  socket.on('call:answer', ({ callId, targetUserId, answer }: any) => {
    io.to(`user:${targetUserId}`).emit('call:answered', { callId, from: userId, answer });
  });

  socket.on('call:reject', ({ callId, targetUserId, reason }: any) => {
    io.to(`user:${targetUserId}`).emit('call:rejected', { callId, by: userId, reason });
  });

  socket.on('call:end', ({ callId, targetUserId }: any) => {
    io.to(`user:${targetUserId}`).emit('call:ended', { callId, by: userId });
    if (callId) socket.leave(`call:${callId}`);
  });

  socket.on('call:join_room', ({ callId }: any) => {
    socket.join(`call:${callId}`);
    socket.to(`call:${callId}`).emit('call:peer_joined', { userId, username });
  });

  socket.on('call:ice_candidate', ({ callId, targetUserId, candidate }: any) => {
    io.to(`user:${targetUserId}`).emit('call:ice_candidate', { callId, candidate, from: userId });
  });

  socket.on('call:sdp_offer', ({ callId, targetUserId, sdp }: any) => {
    io.to(`user:${targetUserId}`).emit('call:sdp_offer', { callId, sdp, from: userId });
  });

  socket.on('call:sdp_answer', ({ callId, targetUserId, sdp }: any) => {
    io.to(`user:${targetUserId}`).emit('call:sdp_answer', { callId, sdp, from: userId });
  });

  socket.on('call:screen_share_start', ({ callId }: any) => {
    socket.to(`call:${callId}`).emit('call:screen_share_started', { userId });
  });

  socket.on('call:screen_share_stop', ({ callId }: any) => {
    socket.to(`call:${callId}`).emit('call:screen_share_stopped', { userId });
  });

  // ── Group call (rooms)
  socket.on('call:mute', ({ callId, isMuted }: any) => {
    socket.to(`call:${callId}`).emit('call:participant_muted', { userId, isMuted });
  });

  socket.on('call:video_toggle', ({ callId, isVideoOff }: any) => {
    socket.to(`call:${callId}`).emit('call:participant_video_changed', { userId, isVideoOff });
  });

  // ── Post real-time events
  socket.on('post:created', ({ postId }: any) => {
    socket.broadcast.emit('feed:new_post', { postId, userId, username });
  });

  socket.on('story:viewed', ({ storyId }: any) => {
    io.to(`user:${userId}`).emit('story:view_ack', { storyId });
  });

  // ── Disconnect
  socket.on('disconnect', async (reason) => {
    console.log(`[WS] ${username} disconnected: ${reason}`);
    const sockets = userSockets.get(userId);
    if (sockets) {
      sockets.delete(socket.id);
      if (sockets.size === 0) {
        userSockets.delete(userId);
        await publisher.del(`socionet:presence:${userId}`);
        await publisher.publish('socionet:presence', JSON.stringify({ userId, status: 'offline' }));
        io.emit('presence:offline', { userId, username });
      }
    }
    socketUser.delete(socket.id);
  });

  socket.on('error', (err) => {
    console.error(`[WS] Socket error for ${username}:`, err.message);
  });
});

// ── Redis pub/sub bridge (API -> Realtime -> Client)
subscriber.subscribe('socionet:notifications', (message) => {
  try {
    const notif = JSON.parse(message);
    io.to(`user:${notif.recipient_id}`).emit('notification:new', notif);
  } catch (err) {
    console.error('[Redis] Notification broadcast error:', err);
  }
});

subscriber.subscribe('socionet:messages', (message) => {
  try {
    const msg = JSON.parse(message);
    io.to(`conv:${msg.conversation_id}`).emit('message:new', msg);
  } catch (err) {
    console.error('[Redis] Message broadcast error:', err);
  }
});

subscriber.subscribe('socionet:presence', (message) => {
  try {
    const { userId, status } = JSON.parse(message);
    io.emit(`presence:${userId}`, { userId, status, ts: Date.now() });
  } catch {}
});

subscriber.subscribe('socionet:feed:update', (message) => {
  try {
    const update = JSON.parse(message);
    if (update.follower_ids) {
      for (const fid of update.follower_ids) {
        io.to(`user:${fid}`).emit('feed:update', update);
      }
    }
  } catch {}
});

// ── Start server
httpServer.listen(PORT, () => {
  console.log(`\n⚡ SOCIONET Realtime Server on :${PORT}`);
  console.log(`👥 Socket.io ready · WebRTC signaling enabled\n`);
});

// ── Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('Realtime server shutting down...');
  io.close();
  await publisher.quit();
  await subscriber.quit();
  process.exit(0);
});

export { io, publisher };
