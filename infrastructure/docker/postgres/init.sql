-- ════════════════════════════════════════════
-- SOCIONET — Complete Database Schema v3.0
-- PostgreSQL 16 — 45+ tables
-- ════════════════════════════════════════════

-- ── ENUMS ──────────────────────────────────
CREATE TYPE user_status AS ENUM ('active','suspended','deactivated','banned');
CREATE TYPE user_gender AS ENUM ('male','female','non_binary','prefer_not_to_say','other');
CREATE TYPE post_type AS ENUM ('text','image','video','carousel','reel','story','live','podcast');
CREATE TYPE post_visibility AS ENUM ('public','followers','close_friends','only_me');
CREATE TYPE relationship_type AS ENUM ('follow','friend','close_friend','blocked','muted','restricted');
CREATE TYPE relationship_status AS ENUM ('pending','accepted','declined');
CREATE TYPE notification_type AS ENUM (
  'like','comment','reply','follow','follow_request','follow_accepted',
  'mention','tag','dm','group_message','channel_message','live_started',
  'story_reaction','story_mention','post_shared','comment_liked',
  'community_invite','community_approved','achievement','system','tip_received'
);
CREATE TYPE message_type AS ENUM ('text','image','video','audio','file','location','sticker','gif','system','poll','voice_note');
CREATE TYPE conversation_type AS ENUM ('direct','group','channel','broadcast','support');
CREATE TYPE media_type AS ENUM ('image','video','audio','document','gif','sticker');
CREATE TYPE reaction_emoji AS ENUM ('like','love','haha','wow','sad','angry','care','fire','clap');
CREATE TYPE report_reason AS ENUM ('spam','nudity','harassment','hate_speech','violence','misinformation','intellectual_property','self_harm','other');
CREATE TYPE report_status AS ENUM ('pending','under_review','actioned','dismissed');
CREATE TYPE call_type AS ENUM ('voice','video','screen_share');
CREATE TYPE call_status AS ENUM ('ringing','active','ended','missed','declined','busy');
CREATE TYPE member_role AS ENUM ('owner','admin','moderator','member');
CREATE TYPE community_status AS ENUM ('active','archived','suspended');
CREATE TYPE wallet_tx_type AS ENUM ('tip','subscription','nft_sale','reward','withdrawal','deposit','refund','fee');
CREATE TYPE nft_status AS ENUM ('minted','listed','sold','burned','soulbound');
CREATE TYPE story_poll_type AS ENUM ('yes_no','multiple_choice','slider','question');

-- ── USERS ──────────────────────────────────
CREATE TABLE users (
  id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  username              VARCHAR(30) UNIQUE NOT NULL,
  email                 VARCHAR(255) UNIQUE,
  phone                 VARCHAR(20) UNIQUE,
  password_hash         VARCHAR(255),
  -- Profile
  full_name             VARCHAR(100) NOT NULL,
  display_name          VARCHAR(100),
  bio                   TEXT,
  website               VARCHAR(255),
  avatar_url            VARCHAR(500),
  banner_url            VARCHAR(500),
  location              VARCHAR(100),
  date_of_birth         DATE,
  gender                user_gender,
  pronouns              VARCHAR(50),
  -- Status
  status                user_status DEFAULT 'active',
  is_verified           BOOLEAN DEFAULT false,
  is_business           BOOLEAN DEFAULT false,
  is_creator            BOOLEAN DEFAULT false,
  is_private            BOOLEAN DEFAULT false,
  is_online             BOOLEAN DEFAULT false,
  last_seen_at          TIMESTAMPTZ,
  -- Settings
  allow_messages_from   VARCHAR(20) DEFAULT 'everyone',
  allow_tags_from       VARCHAR(20) DEFAULT 'everyone',
  show_activity_status  BOOLEAN DEFAULT true,
  show_read_receipts    BOOLEAN DEFAULT true,
  show_suggested        BOOLEAN DEFAULT true,
  two_factor_enabled    BOOLEAN DEFAULT false,
  two_factor_secret     VARCHAR(255),
  -- Counts (denormalized)
  followers_count       INTEGER DEFAULT 0,
  following_count       INTEGER DEFAULT 0,
  posts_count           INTEGER DEFAULT 0,
  -- Identity / Web3
  did                   VARCHAR(255) UNIQUE,
  wallet_address        VARCHAR(255),
  -- Metadata
  created_at            TIMESTAMPTZ DEFAULT NOW(),
  updated_at            TIMESTAMPTZ DEFAULT NOW(),
  deleted_at            TIMESTAMPTZ
);

CREATE INDEX idx_users_username     ON users(LOWER(username));
CREATE INDEX idx_users_email        ON users(LOWER(email));
CREATE INDEX idx_users_phone        ON users(phone);
CREATE INDEX idx_users_status       ON users(status) WHERE status = 'active';
CREATE INDEX idx_users_is_online    ON users(is_online) WHERE is_online = true;
CREATE INDEX idx_users_created      ON users(created_at DESC);
CREATE INDEX idx_users_fts          ON users USING gin(
  to_tsvector('english', COALESCE(full_name,'') || ' ' || COALESCE(username,'') || ' ' || COALESCE(bio,''))
);
CREATE INDEX idx_users_username_trgm ON users USING gin(username gin_trgm_ops);
CREATE INDEX idx_users_name_trgm     ON users USING gin(full_name gin_trgm_ops);

-- ── USER SETTINGS ──
CREATE TABLE user_settings (
  user_id                     UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  theme                       VARCHAR(20) DEFAULT 'dark',
  accent_color                VARCHAR(7) DEFAULT '#7c6af7',
  language                    VARCHAR(10) DEFAULT 'en',
  timezone                    VARCHAR(50) DEFAULT 'UTC',
  date_format                 VARCHAR(20) DEFAULT 'MMM DD, YYYY',
  compact_mode                BOOLEAN DEFAULT false,
  reduce_motion               BOOLEAN DEFAULT false,
  autoplay_videos             BOOLEAN DEFAULT true,
  autoplay_reels              BOOLEAN DEFAULT true,
  data_saver_mode             BOOLEAN DEFAULT false,
  content_filter_level        VARCHAR(20) DEFAULT 'moderate',
  sensitive_content_enabled   BOOLEAN DEFAULT false,
  updated_at                  TIMESTAMPTZ DEFAULT NOW()
);

-- ── AUTH TOKENS ──
CREATE TABLE refresh_tokens (
  id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash  VARCHAR(255) NOT NULL UNIQUE,
  device_name VARCHAR(200),
  device_type VARCHAR(50),
  user_agent  TEXT,
  ip_address  INET,
  expires_at  TIMESTAMPTZ NOT NULL,
  last_used   TIMESTAMPTZ,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_refresh_tokens_user    ON refresh_tokens(user_id);
CREATE INDEX idx_refresh_tokens_expires ON refresh_tokens(expires_at);

CREATE TABLE otp_codes (
  id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  identifier  VARCHAR(255) NOT NULL,
  code_hash   VARCHAR(255) NOT NULL,
  purpose     VARCHAR(50) NOT NULL,
  attempts    INTEGER DEFAULT 0,
  expires_at  TIMESTAMPTZ NOT NULL,
  used_at     TIMESTAMPTZ,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_otp_identifier ON otp_codes(identifier, purpose, expires_at);

CREATE TABLE login_attempts (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  identifier VARCHAR(255) NOT NULL,
  ip_address INET,
  success    BOOLEAN DEFAULT false,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_login_attempts_identifier ON login_attempts(identifier, created_at DESC);

-- ── MEDIA FILES ──
CREATE TABLE media (
  id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id          UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  type             media_type NOT NULL,
  url              VARCHAR(500) NOT NULL,
  thumbnail_url    VARCHAR(500),
  preview_url      VARCHAR(500),
  bucket           VARCHAR(100) NOT NULL,
  key              VARCHAR(500) NOT NULL,
  original_name    VARCHAR(255),
  size_bytes       BIGINT,
  width            INTEGER,
  height           INTEGER,
  duration_seconds FLOAT,
  mime_type        VARCHAR(100),
  blurhash         VARCHAR(100),
  alt_text         VARCHAR(500),
  is_processed     BOOLEAN DEFAULT false,
  metadata         JSONB DEFAULT '{}',
  created_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_media_user ON media(user_id);
CREATE INDEX idx_media_type ON media(type);

-- ── POSTS ──
CREATE TABLE posts (
  id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id           UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  type              post_type NOT NULL DEFAULT 'text',
  visibility        post_visibility DEFAULT 'public',
  -- Content
  caption           TEXT,
  caption_html      TEXT,
  location_name     VARCHAR(255),
  location_lat      DECIMAL(10,7),
  location_lng      DECIMAL(10,7),
  feeling           VARCHAR(100),
  feeling_emoji     VARCHAR(10),
  background_color  VARCHAR(20),
  font_style        VARCHAR(50),
  -- For reposts/quotes
  original_post_id  UUID REFERENCES posts(id) ON DELETE SET NULL,
  quoted_post_id    UUID REFERENCES posts(id) ON DELETE SET NULL,
  is_repost         BOOLEAN DEFAULT false,
  repost_comment    TEXT,
  -- For stories
  expires_at        TIMESTAMPTZ,
  story_duration    INTEGER DEFAULT 5,
  -- For live
  stream_key        VARCHAR(255) UNIQUE,
  stream_url        VARCHAR(500),
  scheduled_at      TIMESTAMPTZ,
  -- Counts (denormalized)
  likes_count       INTEGER DEFAULT 0,
  comments_count    INTEGER DEFAULT 0,
  shares_count      INTEGER DEFAULT 0,
  saves_count       INTEGER DEFAULT 0,
  views_count       INTEGER DEFAULT 0,
  reach_count       INTEGER DEFAULT 0,
  -- Settings
  comments_disabled BOOLEAN DEFAULT false,
  likes_hidden      BOOLEAN DEFAULT false,
  shares_disabled   BOOLEAN DEFAULT false,
  is_pinned         BOOLEAN DEFAULT false,
  is_sensitive      BOOLEAN DEFAULT false,
  is_sponsored      BOOLEAN DEFAULT false,
  edited_at         TIMESTAMPTZ,
  archived_at       TIMESTAMPTZ,
  created_at        TIMESTAMPTZ DEFAULT NOW(),
  updated_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_posts_user         ON posts(user_id, created_at DESC);
CREATE INDEX idx_posts_type         ON posts(type, created_at DESC);
CREATE INDEX idx_posts_visibility   ON posts(visibility) WHERE visibility = 'public';
CREATE INDEX idx_posts_created      ON posts(created_at DESC);
CREATE INDEX idx_posts_expires      ON posts(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_posts_pinned       ON posts(user_id) WHERE is_pinned = true;
CREATE INDEX idx_posts_feed         ON posts(user_id, created_at DESC, visibility) WHERE visibility IN ('public','followers');
CREATE INDEX idx_posts_stories      ON posts(user_id, expires_at) WHERE type = 'story';
CREATE INDEX idx_posts_fts          ON posts USING gin(to_tsvector('english', COALESCE(caption,'')));
CREATE INDEX idx_posts_score        ON posts(likes_count DESC, comments_count DESC, views_count DESC);

-- ── POST MEDIA ──
CREATE TABLE post_media (
  id        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  post_id   UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  media_id  UUID NOT NULL REFERENCES media(id) ON DELETE CASCADE,
  position  SMALLINT DEFAULT 0,
  alt_text  VARCHAR(500),
  link_url  VARCHAR(500),
  crop_data JSONB
);
CREATE INDEX idx_post_media_post ON post_media(post_id, position);
CREATE UNIQUE INDEX idx_post_media_unique ON post_media(post_id, position);

-- ── HASHTAGS ──
CREATE TABLE hashtags (
  id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name         VARCHAR(150) UNIQUE NOT NULL,
  posts_count  INTEGER DEFAULT 0,
  category     VARCHAR(100),
  is_trending  BOOLEAN DEFAULT false,
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  updated_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_hashtags_name  ON hashtags(LOWER(name));
CREATE INDEX idx_hashtags_count ON hashtags(posts_count DESC);
CREATE INDEX idx_hashtags_name_trgm ON hashtags USING gin(name gin_trgm_ops);

CREATE TABLE post_hashtags (
  post_id    UUID REFERENCES posts(id) ON DELETE CASCADE,
  hashtag_id UUID REFERENCES hashtags(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (post_id, hashtag_id)
);
CREATE INDEX idx_post_hashtags_hashtag ON post_hashtags(hashtag_id, created_at DESC);

-- ── MENTIONS ──
CREATE TABLE post_mentions (
  post_id    UUID REFERENCES posts(id) ON DELETE CASCADE,
  user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (post_id, user_id)
);
CREATE INDEX idx_post_mentions_user ON post_mentions(user_id);

-- ── POST REACTIONS ──
CREATE TABLE post_reactions (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  post_id    UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  reaction   reaction_emoji DEFAULT 'like',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (post_id, user_id)
);
CREATE INDEX idx_post_reactions_post ON post_reactions(post_id, reaction);
CREATE INDEX idx_post_reactions_user ON post_reactions(user_id, created_at DESC);

-- ── POST SAVES ──
CREATE TABLE post_saves (
  post_id     UUID REFERENCES posts(id) ON DELETE CASCADE,
  user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
  collection  VARCHAR(100),
  created_at  TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (post_id, user_id)
);
CREATE INDEX idx_post_saves_user ON post_saves(user_id, created_at DESC);

-- ── POST VIEWS ──
CREATE TABLE post_views (
  id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  post_id     UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  user_id     UUID REFERENCES users(id) ON DELETE SET NULL,
  ip_hash     VARCHAR(64),
  duration_ms INTEGER,
  source      VARCHAR(50),
  viewed_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_post_views_post ON post_views(post_id, viewed_at DESC);

-- ── COMMENTS ──
CREATE TABLE comments (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  post_id       UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  user_id       UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  parent_id     UUID REFERENCES comments(id) ON DELETE CASCADE,
  content       TEXT NOT NULL,
  content_html  TEXT,
  likes_count   INTEGER DEFAULT 0,
  replies_count INTEGER DEFAULT 0,
  is_pinned     BOOLEAN DEFAULT false,
  is_author     BOOLEAN DEFAULT false,
  edited_at     TIMESTAMPTZ,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_comments_post   ON comments(post_id, created_at ASC);
CREATE INDEX idx_comments_parent ON comments(parent_id, created_at ASC) WHERE parent_id IS NOT NULL;
CREATE INDEX idx_comments_user   ON comments(user_id, created_at DESC);
CREATE INDEX idx_comments_pinned ON comments(post_id) WHERE is_pinned = true;

CREATE TABLE comment_reactions (
  comment_id UUID REFERENCES comments(id) ON DELETE CASCADE,
  user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
  reaction   reaction_emoji DEFAULT 'like',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (comment_id, user_id)
);

CREATE TABLE comment_mentions (
  comment_id UUID REFERENCES comments(id) ON DELETE CASCADE,
  user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
  PRIMARY KEY (comment_id, user_id)
);

-- ── RELATIONSHIPS ──
CREATE TABLE relationships (
  id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  follower_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  following_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  type         relationship_type NOT NULL DEFAULT 'follow',
  status       relationship_status DEFAULT 'accepted',
  note         VARCHAR(255),
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  updated_at   TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE (follower_id, following_id, type)
);
CREATE INDEX idx_rel_follower  ON relationships(follower_id, type, status);
CREATE INDEX idx_rel_following ON relationships(following_id, type, status);

-- ── STORY INTERACTIONS ──
CREATE TABLE story_views (
  story_id   UUID REFERENCES posts(id) ON DELETE CASCADE,
  viewer_id  UUID REFERENCES users(id) ON DELETE CASCADE,
  viewed_at  TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (story_id, viewer_id)
);
CREATE INDEX idx_story_views_story ON story_views(story_id, viewed_at DESC);

CREATE TABLE story_reactions (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  story_id   UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  emoji      VARCHAR(10),
  message    TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE story_polls (
  id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  story_id    UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  type        story_poll_type DEFAULT 'yes_no',
  question    VARCHAR(300) NOT NULL,
  options     JSONB DEFAULT '[]',
  expires_at  TIMESTAMPTZ,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE story_poll_responses (
  poll_id    UUID REFERENCES story_polls(id) ON DELETE CASCADE,
  user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
  response   JSONB NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (poll_id, user_id)
);

CREATE TABLE story_highlights (
  id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title          VARCHAR(100) NOT NULL,
  cover_media_id UUID REFERENCES media(id) ON DELETE SET NULL,
  position       SMALLINT DEFAULT 0,
  created_at     TIMESTAMPTZ DEFAULT NOW(),
  updated_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_highlights_user ON story_highlights(user_id, position);

CREATE TABLE story_highlight_items (
  highlight_id UUID REFERENCES story_highlights(id) ON DELETE CASCADE,
  post_id      UUID REFERENCES posts(id) ON DELETE CASCADE,
  position     SMALLINT DEFAULT 0,
  added_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (highlight_id, post_id)
);

-- ── CONVERSATIONS ──
CREATE TABLE conversations (
  id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  type                    conversation_type NOT NULL DEFAULT 'direct',
  name                    VARCHAR(100),
  description             TEXT,
  avatar_url              VARCHAR(500),
  created_by              UUID REFERENCES users(id) ON DELETE SET NULL,
  -- Settings
  is_encrypted            BOOLEAN DEFAULT true,
  disappear_after_seconds INTEGER,
  max_members             INTEGER DEFAULT 256,
  invite_link             VARCHAR(100) UNIQUE,
  invite_link_enabled     BOOLEAN DEFAULT false,
  -- Counts
  members_count           INTEGER DEFAULT 0,
  messages_count          INTEGER DEFAULT 0,
  last_message_id         UUID,
  last_activity_at        TIMESTAMPTZ DEFAULT NOW(),
  created_at              TIMESTAMPTZ DEFAULT NOW(),
  updated_at              TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_conv_last_activity ON conversations(last_activity_at DESC);
CREATE INDEX idx_conv_type          ON conversations(type);

CREATE TABLE conversation_members (
  id                   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  conversation_id      UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  user_id              UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role                 member_role DEFAULT 'member',
  nickname             VARCHAR(100),
  is_muted             BOOLEAN DEFAULT false,
  muted_until          TIMESTAMPTZ,
  is_pinned            BOOLEAN DEFAULT false,
  is_archived          BOOLEAN DEFAULT false,
  last_read_message_id UUID,
  unread_count         INTEGER DEFAULT 0,
  notification_sound   VARCHAR(50) DEFAULT 'default',
  joined_at            TIMESTAMPTZ DEFAULT NOW(),
  left_at              TIMESTAMPTZ,
  kicked_by            UUID REFERENCES users(id) ON DELETE SET NULL,
  UNIQUE(conversation_id, user_id)
);
CREATE INDEX idx_conv_members_user ON conversation_members(user_id, is_pinned DESC, last_activity_at DESC);
CREATE INDEX idx_conv_members_conv ON conversation_members(conversation_id);

-- ── MESSAGES ──
CREATE TABLE messages (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  sender_id       UUID REFERENCES users(id) ON DELETE SET NULL,
  type            message_type NOT NULL DEFAULT 'text',
  -- Content
  content         TEXT,
  content_html    TEXT,
  media_id        UUID REFERENCES media(id) ON DELETE SET NULL,
  metadata        JSONB DEFAULT '{}',
  -- Threading
  reply_to_id     UUID REFERENCES messages(id) ON DELETE SET NULL,
  thread_id       UUID REFERENCES messages(id) ON DELETE SET NULL,
  thread_count    INTEGER DEFAULT 0,
  -- Forwarding
  forwarded_from_id       UUID REFERENCES messages(id) ON DELETE SET NULL,
  forwarded_from_conv_id  UUID REFERENCES conversations(id) ON DELETE SET NULL,
  -- State
  is_deleted              BOOLEAN DEFAULT false,
  deleted_for_everyone    BOOLEAN DEFAULT false,
  deleted_by              UUID REFERENCES users(id) ON DELETE SET NULL,
  is_edited               BOOLEAN DEFAULT false,
  edit_history            JSONB DEFAULT '[]',
  is_pinned               BOOLEAN DEFAULT false,
  pinned_by               UUID REFERENCES users(id) ON DELETE SET NULL,
  pinned_at               TIMESTAMPTZ,
  expires_at              TIMESTAMPTZ,
  scheduled_at            TIMESTAMPTZ,
  -- Delivery
  delivered_count         INTEGER DEFAULT 0,
  read_count              INTEGER DEFAULT 0,
  created_at              TIMESTAMPTZ DEFAULT NOW(),
  edited_at               TIMESTAMPTZ
);
CREATE INDEX idx_messages_conv    ON messages(conversation_id, created_at ASC) WHERE is_deleted = false;
CREATE INDEX idx_messages_sender  ON messages(sender_id, created_at DESC);
CREATE INDEX idx_messages_thread  ON messages(thread_id) WHERE thread_id IS NOT NULL;
CREATE INDEX idx_messages_expires ON messages(expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX idx_messages_pinned  ON messages(conversation_id) WHERE is_pinned = true;
CREATE INDEX idx_messages_fts     ON messages USING gin(to_tsvector('english', COALESCE(content,'')));

CREATE TABLE message_receipts (
  message_id   UUID REFERENCES messages(id) ON DELETE CASCADE,
  user_id      UUID REFERENCES users(id) ON DELETE CASCADE,
  delivered_at TIMESTAMPTZ,
  read_at      TIMESTAMPTZ,
  PRIMARY KEY (message_id, user_id)
);
CREATE INDEX idx_receipts_user ON message_receipts(user_id, read_at);

CREATE TABLE message_reactions (
  message_id UUID REFERENCES messages(id) ON DELETE CASCADE,
  user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
  emoji      VARCHAR(10) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (message_id, user_id)
);

-- ── CALLS ──
CREATE TABLE calls (
  id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  conversation_id     UUID REFERENCES conversations(id) ON DELETE SET NULL,
  initiator_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  type                call_type NOT NULL DEFAULT 'voice',
  status              call_status DEFAULT 'ringing',
  room_id             VARCHAR(255) UNIQUE,
  rtc_offer           TEXT,
  max_participants    INTEGER DEFAULT 50,
  is_encrypted        BOOLEAN DEFAULT true,
  recording_enabled   BOOLEAN DEFAULT false,
  recording_url       VARCHAR(500),
  started_at          TIMESTAMPTZ,
  ended_at            TIMESTAMPTZ,
  duration_seconds    INTEGER,
  quality_score       DECIMAL(3,2),
  created_at          TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_calls_initiator ON calls(initiator_id, created_at DESC);
CREATE INDEX idx_calls_status    ON calls(status) WHERE status IN ('ringing','active');

CREATE TABLE call_participants (
  call_id       UUID REFERENCES calls(id) ON DELETE CASCADE,
  user_id       UUID REFERENCES users(id) ON DELETE CASCADE,
  joined_at     TIMESTAMPTZ,
  left_at       TIMESTAMPTZ,
  is_muted      BOOLEAN DEFAULT false,
  is_video_off  BOOLEAN DEFAULT false,
  is_sharing    BOOLEAN DEFAULT false,
  connection_quality VARCHAR(20),
  PRIMARY KEY (call_id, user_id)
);

-- ── NOTIFICATIONS ──
CREATE TABLE notifications (
  id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  recipient_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  sender_id    UUID REFERENCES users(id) ON DELETE SET NULL,
  type         notification_type NOT NULL,
  entity_type  VARCHAR(50),
  entity_id    UUID,
  title        VARCHAR(255),
  body         TEXT,
  image_url    VARCHAR(500),
  action_url   VARCHAR(500),
  data         JSONB DEFAULT '{}',
  is_read      BOOLEAN DEFAULT false,
  is_seen      BOOLEAN DEFAULT false,
  read_at      TIMESTAMPTZ,
  grouped_with UUID REFERENCES notifications(id) ON DELETE SET NULL,
  group_count  INTEGER DEFAULT 1,
  created_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_notif_recipient ON notifications(recipient_id, is_read, created_at DESC);
CREATE INDEX idx_notif_created   ON notifications(created_at DESC);
CREATE INDEX idx_notif_entity    ON notifications(entity_type, entity_id);

CREATE TABLE notification_preferences (
  user_id              UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  likes                BOOLEAN DEFAULT true,
  comments             BOOLEAN DEFAULT true,
  replies              BOOLEAN DEFAULT true,
  follows              BOOLEAN DEFAULT true,
  mentions             BOOLEAN DEFAULT true,
  messages             BOOLEAN DEFAULT true,
  group_messages       BOOLEAN DEFAULT true,
  live_started         BOOLEAN DEFAULT true,
  story_reactions      BOOLEAN DEFAULT true,
  community_activity   BOOLEAN DEFAULT true,
  achievements         BOOLEAN DEFAULT true,
  tips_received        BOOLEAN DEFAULT true,
  email_digest         BOOLEAN DEFAULT false,
  email_marketing      BOOLEAN DEFAULT false,
  push_enabled         BOOLEAN DEFAULT true,
  push_sound           BOOLEAN DEFAULT true,
  push_vibration       BOOLEAN DEFAULT true,
  quiet_hours_enabled  BOOLEAN DEFAULT false,
  quiet_hours_start    TIME,
  quiet_hours_end      TIME,
  updated_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE push_tokens (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token      TEXT NOT NULL UNIQUE,
  platform   VARCHAR(20) NOT NULL DEFAULT 'web',
  device_id  VARCHAR(255),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_push_tokens_user ON push_tokens(user_id);

-- ── COMMUNITIES ──
CREATE TABLE communities (
  id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name              VARCHAR(100) NOT NULL,
  slug              VARCHAR(120) UNIQUE NOT NULL,
  description       TEXT,
  long_description  TEXT,
  avatar_url        VARCHAR(500),
  banner_url        VARCHAR(500),
  category          VARCHAR(100),
  tags              TEXT[],
  rules             JSONB DEFAULT '[]',
  status            community_status DEFAULT 'active',
  -- Settings
  is_private        BOOLEAN DEFAULT false,
  is_verified       BOOLEAN DEFAULT false,
  is_nsfw           BOOLEAN DEFAULT false,
  requires_approval BOOLEAN DEFAULT false,
  allow_posts_by    VARCHAR(20) DEFAULT 'members',
  -- Counts
  members_count     INTEGER DEFAULT 0,
  posts_count       INTEGER DEFAULT 0,
  online_count      INTEGER DEFAULT 0,
  -- Metadata
  created_by        UUID REFERENCES users(id) ON DELETE SET NULL,
  created_at        TIMESTAMPTZ DEFAULT NOW(),
  updated_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_comm_slug     ON communities(slug);
CREATE INDEX idx_comm_members  ON communities(members_count DESC);
CREATE INDEX idx_comm_category ON communities(category);
CREATE INDEX idx_comm_name_trgm ON communities USING gin(name gin_trgm_ops);

CREATE TABLE community_members (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  community_id  UUID NOT NULL REFERENCES communities(id) ON DELETE CASCADE,
  user_id       UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  role          member_role DEFAULT 'member',
  status        VARCHAR(20) DEFAULT 'active',
  badges        TEXT[],
  post_count    INTEGER DEFAULT 0,
  joined_at     TIMESTAMPTZ DEFAULT NOW(),
  banned_at     TIMESTAMPTZ,
  banned_by     UUID REFERENCES users(id) ON DELETE SET NULL,
  ban_reason    TEXT,
  UNIQUE(community_id, user_id)
);
CREATE INDEX idx_comm_members_comm ON community_members(community_id, role);
CREATE INDEX idx_comm_members_user ON community_members(user_id, joined_at DESC);

CREATE TABLE community_posts (
  community_id UUID REFERENCES communities(id) ON DELETE CASCADE,
  post_id      UUID REFERENCES posts(id) ON DELETE CASCADE,
  posted_by    UUID REFERENCES users(id) ON DELETE SET NULL,
  is_pinned    BOOLEAN DEFAULT false,
  is_featured  BOOLEAN DEFAULT false,
  approved_by  UUID REFERENCES users(id) ON DELETE SET NULL,
  approved_at  TIMESTAMPTZ,
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (community_id, post_id)
);
CREATE INDEX idx_comm_posts_community ON community_posts(community_id, is_pinned DESC, created_at DESC);

CREATE TABLE community_bans (
  id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  community_id UUID NOT NULL REFERENCES communities(id) ON DELETE CASCADE,
  user_id      UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  banned_by    UUID REFERENCES users(id) ON DELETE SET NULL,
  reason       TEXT,
  expires_at   TIMESTAMPTZ,
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(community_id, user_id)
);

-- ── LIVE STREAMS ──
CREATE TABLE live_streams (
  id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  post_id          UUID REFERENCES posts(id) ON DELETE CASCADE,
  user_id          UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title            VARCHAR(255) NOT NULL,
  description      TEXT,
  category         VARCHAR(100),
  tags             TEXT[],
  thumbnail_url    VARCHAR(500),
  stream_key       VARCHAR(255) UNIQUE NOT NULL,
  rtmp_url         VARCHAR(500),
  playback_url     VARCHAR(500),
  hls_url          VARCHAR(500),
  status           VARCHAR(20) DEFAULT 'idle',
  is_recorded      BOOLEAN DEFAULT false,
  recording_url    VARCHAR(500),
  peak_viewers     INTEGER DEFAULT 0,
  total_viewers    INTEGER DEFAULT 0,
  total_reactions  INTEGER DEFAULT 0,
  total_comments   INTEGER DEFAULT 0,
  started_at       TIMESTAMPTZ,
  ended_at         TIMESTAMPTZ,
  duration_seconds INTEGER,
  created_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_live_user   ON live_streams(user_id);
CREATE INDEX idx_live_status ON live_streams(status, started_at DESC);

CREATE TABLE live_viewers (
  stream_id  UUID REFERENCES live_streams(id) ON DELETE CASCADE,
  user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
  joined_at  TIMESTAMPTZ DEFAULT NOW(),
  left_at    TIMESTAMPTZ,
  PRIMARY KEY (stream_id, user_id)
);

CREATE TABLE live_comments (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  stream_id  UUID NOT NULL REFERENCES live_streams(id) ON DELETE CASCADE,
  user_id    UUID REFERENCES users(id) ON DELETE SET NULL,
  content    VARCHAR(500) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_live_comments ON live_comments(stream_id, created_at DESC);

CREATE TABLE live_gifts (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  stream_id  UUID NOT NULL REFERENCES live_streams(id) ON DELETE CASCADE,
  sender_id  UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  gift_type  VARCHAR(100) NOT NULL,
  quantity   INTEGER DEFAULT 1,
  tokens     INTEGER NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ── SEARCH ──
CREATE TABLE search_history (
  id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  query       VARCHAR(255),
  result_type VARCHAR(50),
  result_id   UUID,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_search_hist_user ON search_history(user_id, created_at DESC);

CREATE TABLE trending_topics (
  id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  topic        VARCHAR(150) NOT NULL,
  hashtag_id   UUID REFERENCES hashtags(id) ON DELETE CASCADE,
  category     VARCHAR(100),
  posts_24h    INTEGER DEFAULT 0,
  posts_1h     INTEGER DEFAULT 0,
  velocity     FLOAT DEFAULT 0,
  region       VARCHAR(50) DEFAULT 'global',
  rank         INTEGER,
  period_start TIMESTAMPTZ,
  period_end   TIMESTAMPTZ,
  computed_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_trending_rank ON trending_topics(rank, computed_at DESC);

-- ── FEED ALGORITHM ──
CREATE TABLE user_interests (
  user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
  topic      VARCHAR(100) NOT NULL,
  score      FLOAT DEFAULT 1.0,
  source     VARCHAR(50) DEFAULT 'implicit',
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (user_id, topic)
);

CREATE TABLE post_scores (
  post_id          UUID PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,
  score            FLOAT DEFAULT 0,
  recency_score    FLOAT DEFAULT 0,
  engagement_score FLOAT DEFAULT 0,
  relevance_score  FLOAT DEFAULT 0,
  viral_score      FLOAT DEFAULT 0,
  computed_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── REPORTS ──
CREATE TABLE reports (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  reporter_id   UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  entity_type   VARCHAR(50) NOT NULL,
  entity_id     UUID NOT NULL,
  reason        report_reason NOT NULL,
  description   TEXT,
  evidence_urls TEXT[],
  status        report_status DEFAULT 'pending',
  priority      SMALLINT DEFAULT 1,
  reviewed_by   UUID REFERENCES users(id) ON DELETE SET NULL,
  reviewed_at   TIMESTAMPTZ,
  action_taken  TEXT,
  created_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_reports_status   ON reports(status, priority DESC, created_at ASC);
CREATE INDEX idx_reports_entity   ON reports(entity_type, entity_id);
CREATE INDEX idx_reports_reporter ON reports(reporter_id);

-- ── WALLET & TOKENS ──
CREATE TABLE wallets (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id         UUID UNIQUE NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  balance_tokens  BIGINT DEFAULT 0,
  balance_usd     DECIMAL(15,4) DEFAULT 0,
  total_earned    BIGINT DEFAULT 0,
  total_spent     BIGINT DEFAULT 0,
  wallet_address  VARCHAR(255),
  chain           VARCHAR(50) DEFAULT 'icp',
  is_verified     BOOLEAN DEFAULT false,
  created_at      TIMESTAMPTZ DEFAULT NOW(),
  updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE wallet_transactions (
  id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  wallet_id      UUID NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
  type           wallet_tx_type NOT NULL,
  amount_tokens  BIGINT NOT NULL,
  amount_usd     DECIMAL(15,4),
  fee_tokens     BIGINT DEFAULT 0,
  from_user_id   UUID REFERENCES users(id) ON DELETE SET NULL,
  to_user_id     UUID REFERENCES users(id) ON DELETE SET NULL,
  reference_type VARCHAR(50),
  reference_id   UUID,
  status         VARCHAR(20) DEFAULT 'completed',
  tx_hash        VARCHAR(255),
  note           TEXT,
  metadata       JSONB DEFAULT '{}',
  created_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_wallet_tx_wallet  ON wallet_transactions(wallet_id, created_at DESC);
CREATE INDEX idx_wallet_tx_status  ON wallet_transactions(status);
CREATE INDEX idx_wallet_tx_from    ON wallet_transactions(from_user_id, created_at DESC);

-- ── NFTs ──
CREATE TABLE nfts (
  id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  owner_id       UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  creator_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name           VARCHAR(255) NOT NULL,
  description    TEXT,
  image_url      VARCHAR(500),
  metadata_url   VARCHAR(500),
  token_id       VARCHAR(255) UNIQUE,
  contract_addr  VARCHAR(255),
  chain          VARCHAR(50) DEFAULT 'icp',
  status         nft_status DEFAULT 'minted',
  price_tokens   BIGINT,
  royalty_pct    DECIMAL(5,2) DEFAULT 10.0,
  collection_id  UUID,
  attributes     JSONB DEFAULT '[]',
  created_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_nfts_owner   ON nfts(owner_id);
CREATE INDEX idx_nfts_creator ON nfts(creator_id);
CREATE INDEX idx_nfts_status  ON nfts(status);

-- ── CREATOR SUBSCRIPTIONS ──
CREATE TABLE creator_plans (
  id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  creator_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name           VARCHAR(100) NOT NULL,
  description    TEXT,
  price_monthly  DECIMAL(10,2) NOT NULL,
  price_yearly   DECIMAL(10,2),
  perks          JSONB DEFAULT '[]',
  is_active      BOOLEAN DEFAULT true,
  subscribers    INTEGER DEFAULT 0,
  created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE creator_subscriptions (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  subscriber_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  creator_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  plan_id       UUID REFERENCES creator_plans(id) ON DELETE SET NULL,
  status        VARCHAR(20) DEFAULT 'active',
  price_monthly DECIMAL(10,2),
  started_at    TIMESTAMPTZ DEFAULT NOW(),
  expires_at    TIMESTAMPTZ,
  auto_renew    BOOLEAN DEFAULT true,
  UNIQUE(subscriber_id, creator_id)
);
CREATE INDEX idx_subscriptions_creator ON creator_subscriptions(creator_id, status);
CREATE INDEX idx_subscriptions_sub     ON creator_subscriptions(subscriber_id, status);

-- ── AI ASSISTANT ──
CREATE TABLE ai_conversations (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title      VARCHAR(255),
  model      VARCHAR(100) DEFAULT 'gpt-4o-mini',
  tokens_used INTEGER DEFAULT 0,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_ai_conv_user ON ai_conversations(user_id, updated_at DESC);

CREATE TABLE ai_messages (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  conversation_id UUID NOT NULL REFERENCES ai_conversations(id) ON DELETE CASCADE,
  role            VARCHAR(20) NOT NULL,
  content         TEXT NOT NULL,
  tokens          INTEGER DEFAULT 0,
  metadata        JSONB DEFAULT '{}',
  created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_ai_messages_conv ON ai_messages(conversation_id, created_at ASC);

-- ── CONTENT COLLECTIONS / PLAYLISTS ──
CREATE TABLE collections (
  id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name        VARCHAR(100) NOT NULL,
  description TEXT,
  cover_url   VARCHAR(500),
  is_private  BOOLEAN DEFAULT false,
  type        VARCHAR(50) DEFAULT 'saved',
  created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_collections_user ON collections(user_id);

CREATE TABLE collection_items (
  collection_id UUID REFERENCES collections(id) ON DELETE CASCADE,
  post_id       UUID REFERENCES posts(id) ON DELETE CASCADE,
  position      INTEGER DEFAULT 0,
  added_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (collection_id, post_id)
);

-- ── POLLS (in posts) ──
CREATE TABLE post_polls (
  id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  post_id     UUID NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  question    VARCHAR(500) NOT NULL,
  options     JSONB NOT NULL DEFAULT '[]',
  expires_at  TIMESTAMPTZ,
  total_votes INTEGER DEFAULT 0,
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE post_poll_votes (
  poll_id    UUID REFERENCES post_polls(id) ON DELETE CASCADE,
  user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
  option_idx SMALLINT NOT NULL,
  voted_at   TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (poll_id, user_id)
);

-- ── LOCATIONS ──
CREATE TABLE user_locations (
  user_id     UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
  lat         DECIMAL(10,7),
  lng         DECIMAL(10,7),
  accuracy_m  INTEGER,
  city        VARCHAR(100),
  country     VARCHAR(100),
  share_mode  VARCHAR(20) DEFAULT 'none',
  updated_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE location_shares (
  id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  sharer_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  recipient_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  expires_at   TIMESTAMPTZ,
  created_at   TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(sharer_id, recipient_id)
);

-- ── ACHIEVEMENTS ──
CREATE TABLE achievements (
  id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  code        VARCHAR(100) UNIQUE NOT NULL,
  name        VARCHAR(200) NOT NULL,
  description TEXT,
  icon_url    VARCHAR(500),
  badge_color VARCHAR(20),
  xp_reward   INTEGER DEFAULT 0,
  tokens_reward BIGINT DEFAULT 0,
  rarity      VARCHAR(20) DEFAULT 'common',
  created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE user_achievements (
  user_id        UUID REFERENCES users(id) ON DELETE CASCADE,
  achievement_id UUID REFERENCES achievements(id) ON DELETE CASCADE,
  unlocked_at    TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (user_id, achievement_id)
);

-- ── AUDIT LOG ──
CREATE TABLE audit_log (
  id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id    UUID REFERENCES users(id) ON DELETE SET NULL,
  action     VARCHAR(100) NOT NULL,
  entity     VARCHAR(50),
  entity_id  UUID,
  old_data   JSONB,
  new_data   JSONB,
  ip_address INET,
  user_agent TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_audit_user   ON audit_log(user_id, created_at DESC);
CREATE INDEX idx_audit_action ON audit_log(action, created_at DESC);

-- ════════════════════════════════════════════
-- TRIGGERS & FUNCTIONS
-- ════════════════════════════════════════════

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_users_updated        BEFORE UPDATE ON users        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_posts_updated        BEFORE UPDATE ON posts        FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_communities_updated  BEFORE UPDATE ON communities  FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_conversations_updated BEFORE UPDATE ON conversations FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_hashtags_updated     BEFORE UPDATE ON hashtags     FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Post count
CREATE OR REPLACE FUNCTION fn_post_count()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' AND NEW.type NOT IN ('story') THEN
    UPDATE users SET posts_count = posts_count + 1 WHERE id = NEW.user_id;
  ELSIF TG_OP = 'DELETE' AND OLD.type NOT IN ('story') THEN
    UPDATE users SET posts_count = GREATEST(0, posts_count - 1) WHERE id = OLD.user_id;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_post_count AFTER INSERT OR DELETE ON posts FOR EACH ROW EXECUTE FUNCTION fn_post_count();

-- Follow counts
CREATE OR REPLACE FUNCTION fn_follow_counts()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' AND NEW.type = 'follow' AND NEW.status = 'accepted' THEN
    UPDATE users SET following_count = following_count + 1 WHERE id = NEW.follower_id;
    UPDATE users SET followers_count = followers_count + 1 WHERE id = NEW.following_id;
  ELSIF TG_OP = 'DELETE' AND OLD.type = 'follow' AND OLD.status = 'accepted' THEN
    UPDATE users SET following_count = GREATEST(0, following_count - 1) WHERE id = OLD.follower_id;
    UPDATE users SET followers_count = GREATEST(0, followers_count - 1) WHERE id = OLD.following_id;
  ELSIF TG_OP = 'UPDATE' AND NEW.type = 'follow' AND OLD.status = 'pending' AND NEW.status = 'accepted' THEN
    UPDATE users SET following_count = following_count + 1 WHERE id = NEW.follower_id;
    UPDATE users SET followers_count = followers_count + 1 WHERE id = NEW.following_id;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_follow_counts AFTER INSERT OR UPDATE OR DELETE ON relationships FOR EACH ROW EXECUTE FUNCTION fn_follow_counts();

-- Post reaction counts
CREATE OR REPLACE FUNCTION fn_post_reaction_counts()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    UPDATE posts SET likes_count = likes_count + 1 WHERE id = NEW.post_id;
  ELSIF TG_OP = 'DELETE' THEN
    UPDATE posts SET likes_count = GREATEST(0, likes_count - 1) WHERE id = OLD.post_id;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_post_reaction_counts AFTER INSERT OR DELETE ON post_reactions FOR EACH ROW EXECUTE FUNCTION fn_post_reaction_counts();

-- Comment counts
CREATE OR REPLACE FUNCTION fn_comment_counts()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    UPDATE posts SET comments_count = comments_count + 1 WHERE id = NEW.post_id;
    IF NEW.parent_id IS NOT NULL THEN
      UPDATE comments SET replies_count = replies_count + 1 WHERE id = NEW.parent_id;
    END IF;
  ELSIF TG_OP = 'DELETE' THEN
    UPDATE posts SET comments_count = GREATEST(0, comments_count - 1) WHERE id = OLD.post_id;
    IF OLD.parent_id IS NOT NULL THEN
      UPDATE comments SET replies_count = GREATEST(0, replies_count - 1) WHERE id = OLD.parent_id;
    END IF;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_comment_counts AFTER INSERT OR DELETE ON comments FOR EACH ROW EXECUTE FUNCTION fn_comment_counts();

-- Comment reaction counts
CREATE OR REPLACE FUNCTION fn_comment_reaction_counts()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    UPDATE comments SET likes_count = likes_count + 1 WHERE id = NEW.comment_id;
  ELSIF TG_OP = 'DELETE' THEN
    UPDATE comments SET likes_count = GREATEST(0, likes_count - 1) WHERE id = OLD.comment_id;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_comment_reaction_counts AFTER INSERT OR DELETE ON comment_reactions FOR EACH ROW EXECUTE FUNCTION fn_comment_reaction_counts();

-- Post saves count
CREATE OR REPLACE FUNCTION fn_post_save_counts()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    UPDATE posts SET saves_count = saves_count + 1 WHERE id = NEW.post_id;
  ELSIF TG_OP = 'DELETE' THEN
    UPDATE posts SET saves_count = GREATEST(0, saves_count - 1) WHERE id = OLD.post_id;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_post_save_counts AFTER INSERT OR DELETE ON post_saves FOR EACH ROW EXECUTE FUNCTION fn_post_save_counts();

-- Community member counts
CREATE OR REPLACE FUNCTION fn_community_member_counts()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' AND NEW.status = 'active' THEN
    UPDATE communities SET members_count = members_count + 1 WHERE id = NEW.community_id;
  ELSIF TG_OP = 'DELETE' AND OLD.status = 'active' THEN
    UPDATE communities SET members_count = GREATEST(0, members_count - 1) WHERE id = OLD.community_id;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_community_member_counts AFTER INSERT OR DELETE ON community_members FOR EACH ROW EXECUTE FUNCTION fn_community_member_counts();

-- Conversation last activity
CREATE OR REPLACE FUNCTION fn_conversation_last_activity()
RETURNS TRIGGER AS $$
BEGIN
  UPDATE conversations SET
    last_message_id = NEW.id,
    last_activity_at = NEW.created_at,
    messages_count = messages_count + 1
  WHERE id = NEW.conversation_id;

  UPDATE conversation_members SET
    unread_count = unread_count + 1
  WHERE conversation_id = NEW.conversation_id
    AND user_id != NEW.sender_id
    AND left_at IS NULL;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_conv_last_activity AFTER INSERT ON messages FOR EACH ROW EXECUTE FUNCTION fn_conversation_last_activity();

-- Hashtag post counts
CREATE OR REPLACE FUNCTION fn_hashtag_counts()
RETURNS TRIGGER AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    UPDATE hashtags SET posts_count = posts_count + 1 WHERE id = NEW.hashtag_id;
  ELSIF TG_OP = 'DELETE' THEN
    UPDATE hashtags SET posts_count = GREATEST(0, posts_count - 1) WHERE id = OLD.hashtag_id;
  END IF;
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_hashtag_counts AFTER INSERT OR DELETE ON post_hashtags FOR EACH ROW EXECUTE FUNCTION fn_hashtag_counts();

-- Wallet balance guard
CREATE OR REPLACE FUNCTION fn_wallet_balance_check()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.balance_tokens < 0 THEN
    RAISE EXCEPTION 'Wallet balance cannot be negative';
  END IF;
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_wallet_balance BEFORE UPDATE ON wallets FOR EACH ROW EXECUTE FUNCTION fn_wallet_balance_check();
