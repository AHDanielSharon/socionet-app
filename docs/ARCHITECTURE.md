# SOCIONET Architecture

## System Components
```
┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Next.js   │────▶│    Nginx     │────▶│  Express API    │
│   (Web UI)  │     │  (Reverse    │     │  (REST + Auth)  │
│  Port 3000  │     │   Proxy)     │     │  Port 4000      │
└─────────────┘     └──────────────┘     └────────┬────────┘
                            │                      │
                    ┌───────▼──────┐      ┌────────▼────────┐
                    │  Socket.io   │      │   PostgreSQL 16  │
                    │  (Realtime)  │      │   (Primary DB)   │
                    │  Port 4001   │      │   Port 5432      │
                    └───────┬──────┘      └─────────────────┘
                            │
              ┌─────────────┴──────────────┐
              │                            │
     ┌────────▼───────┐          ┌────────▼───────┐
     │   Redis 7      │          │   MinIO        │
     │  (Cache +      │          │  (S3-compat    │
     │   Pub/Sub +    │          │   Storage)     │
     │   Presence)    │          │   Port 9000    │
     │   Port 6379    │          └────────────────┘
     └────────────────┘

## Data Flow
1. Client connects to Nginx
2. REST requests → API server
3. WebSocket connections → Realtime server
4. Both API + Realtime pub/sub via Redis
5. Media stored in MinIO (compatible with S3)
6. All persistent data in PostgreSQL

## Feed Algorithm
Weights: own(1.0) → following(0.9) → community(0.8) → discover(0.3)
Score = base_weight × ln(likes×2 + comments×3 + shares + views×0.1) × time_decay
Time decay: exponential based on hours since posted

## Real-time Architecture
- Socket.io with Redis adapter for horizontal scaling
- Presence tracked in Redis with TTL
- Messages delivered via pub/sub channels
- WebRTC signaling for calls (no TURN server needed for LAN)
```
