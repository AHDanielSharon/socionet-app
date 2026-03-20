# SOCIONET API Reference

Base URL: `http://localhost:4000/api/v1`

## Authentication
All protected endpoints require: `Authorization: Bearer <access_token>`
Refresh token: `X-Refresh-Token: <refresh_token>`

## Auth Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /auth/register | Create account |
| POST | /auth/login | Login |
| POST | /auth/logout | Logout |
| POST | /auth/refresh | Refresh tokens |
| POST | /auth/send-otp | Send OTP code |
| POST | /auth/verify-otp | Verify OTP |
| POST | /auth/forgot-password | Request reset |
| POST | /auth/reset-password | Reset password |
| GET | /auth/me | Get current user |

## Posts
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /posts/saved | Get saved posts |
| GET | /posts/:id | Get post |
| POST | /posts | Create post (multipart) |
| PATCH | /posts/:id | Update post |
| DELETE | /posts/:id | Delete post |
| POST | /posts/:id/like | Like/unlike post |
| POST | /posts/:id/save | Save/unsave post |
| POST | /posts/:id/share | Share post |
| GET | /posts/:id/comments | Get comments |
| POST | /posts/:id/comments | Add comment |
| DELETE | /posts/:id/comments/:commentId | Delete comment |
| POST | /posts/:id/comments/:commentId/like | Like comment |
| GET | /posts/:id/likes | List likers |

## Feed
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /feed | Home feed (algorithmic) |
| GET | /feed/reels | Reels feed |
| GET | /feed/stories | Stories feed |
| GET | /feed/videos | Videos feed |

## Users
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /users/me/suggestions | Suggested users |
| GET | /users/:username | User profile |
| GET | /users/:username/posts | User posts |
| GET | /users/:username/followers | Followers list |
| GET | /users/:username/following | Following list |
| PATCH | /users/me/profile | Update profile |
| PATCH | /users/me/username | Change username |
| DELETE | /users/me/account | Delete account |

## Relationships
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /relationships/follow/:userId | Follow user |
| DELETE | /relationships/follow/:userId | Unfollow user |
| POST | /relationships/follow-requests/:userId/accept | Accept follow request |
| DELETE | /relationships/follow-requests/:userId/decline | Decline request |
| GET | /relationships/follow-requests | List follow requests |
| POST | /relationships/block/:userId | Block user |
| DELETE | /relationships/block/:userId | Unblock user |
| GET | /relationships/blocked | List blocked users |

## Search
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /search?q=&type= | Global search |
| GET | /search/trending | Trending topics |
| GET | /search/hashtag/:name | Hashtag page |
| GET | /search/history | Search history |
| DELETE | /search/history | Clear history |

## AI
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /ai/status | Check AI availability |
| GET | /ai/conversations | List conversations |
| POST | /ai/conversations | Create conversation |
| DELETE | /ai/conversations/:id | Delete conversation |
| GET | /ai/conversations/:id/messages | Get messages |
| POST | /ai/conversations/:id/messages | Send message |
| POST | /ai/quick | Quick AI task |

## WebSocket Events (Socket.io)
Connect to: `ws://localhost:4001`

### Client → Server
- `conversation:join` { conversationId }
- `typing:start` { conversationId }
- `typing:stop` { conversationId }
- `messages:read` { conversationId, messageIds }
- `presence:ping`
- `call:initiate` { targetUserId, callType, callId }
- `call:answer` { callId, targetUserId, answer }
- `call:reject` { callId, targetUserId }
- `call:end` { callId, targetUserId }
- `call:ice_candidate` { callId, targetUserId, candidate }
- `call:sdp_offer` { callId, targetUserId, sdp }
- `call:sdp_answer` { callId, targetUserId, sdp }
- `live:join` (streamId)
- `live:leave` (streamId)
- `live:reaction` { streamId, emoji }
- `live:comment` { streamId, content }

### Server → Client
- `message:new` { conversation_id, ... }
- `typing:update` { conversationId, userId, isTyping }
- `notification:new` { id, type, title, body, ... }
- `presence:online` { userId, username }
- `presence:offline` { userId, username }
- `call:incoming` { callId, callType, from }
- `call:answered` { callId, from, answer }
- `call:rejected` { callId, by }
- `call:ended` { callId, by }
- `live:reaction` { userId, emoji }
- `live:comment` { userId, content }
- `live:viewer_count` { streamId, count }
