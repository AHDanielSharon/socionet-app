export interface User {
  id: string;
  username: string;
  email?: string;
  phone?: string;
  full_name: string;
  display_name?: string;
  bio?: string;
  website?: string;
  avatar_url?: string;
  banner_url?: string;
  location?: string;
  date_of_birth?: string;
  gender?: string;
  is_verified: boolean;
  is_private: boolean;
  is_online: boolean;
  is_creator: boolean;
  is_business: boolean;
  followers_count: number;
  following_count: number;
  posts_count: number;
  last_seen_at?: string;
  allow_messages_from: string;
  show_activity_status: boolean;
  created_at: string;
  // Viewer-specific
  is_following?: boolean;
  follows_you?: boolean;
  is_blocked?: boolean;
  follow_requested?: boolean;
}

export interface Post {
  id: string;
  user_id: string;
  type: 'text' | 'image' | 'video' | 'carousel' | 'reel' | 'story' | 'live';
  visibility: 'public' | 'followers' | 'close_friends' | 'only_me';
  caption?: string;
  location_name?: string;
  feeling?: string;
  media?: Media[];
  likes_count: number;
  comments_count: number;
  shares_count: number;
  saves_count: number;
  views_count: number;
  is_liked?: boolean;
  is_saved?: boolean;
  my_reaction?: string;
  is_pinned: boolean;
  comments_disabled: boolean;
  likes_hidden: boolean;
  edited_at?: string;
  created_at: string;
  username: string;
  full_name: string;
  avatar_url?: string;
  is_verified: boolean;
  // Repost
  is_repost?: boolean;
  quoted_post?: Partial<Post>;
}

export interface Media {
  id: string;
  type: 'image' | 'video' | 'audio' | 'document' | 'gif';
  url: string;
  thumbnail_url?: string;
  width?: number;
  height?: number;
  duration_seconds?: number;
  blurhash?: string;
  alt_text?: string;
  position?: number;
}

export interface Comment {
  id: string;
  post_id: string;
  user_id: string;
  parent_id?: string;
  content: string;
  likes_count: number;
  replies_count: number;
  is_pinned: boolean;
  is_liked?: boolean;
  edited_at?: string;
  created_at: string;
  username: string;
  full_name: string;
  avatar_url?: string;
  is_verified: boolean;
}

export interface Conversation {
  id: string;
  type: 'direct' | 'group' | 'channel' | 'broadcast';
  name?: string;
  avatar_url?: string;
  last_message?: Message;
  last_activity_at: string;
  unread_count: number;
  is_muted: boolean;
  is_pinned: boolean;
  members_count: number;
  is_encrypted: boolean;
  other_user?: User;
}

export interface Message {
  id: string;
  conversation_id: string;
  sender_id?: string;
  type: 'text' | 'image' | 'video' | 'audio' | 'file' | 'location' | 'sticker' | 'gif' | 'system';
  content?: string;
  media?: Media;
  reply_to?: Partial<Message>;
  reactions?: MessageReaction[];
  read_by?: Array<{ user_id: string; read_at: string }>;
  is_deleted: boolean;
  is_edited: boolean;
  expires_at?: string;
  created_at: string;
  edited_at?: string;
  sender_username?: string;
  sender_name?: string;
  sender_avatar?: string;
}

export interface MessageReaction {
  user_id: string;
  username?: string;
  emoji: string;
}

export interface Notification {
  id: string;
  recipient_id: string;
  sender_id?: string;
  type: string;
  entity_type?: string;
  entity_id?: string;
  title?: string;
  body?: string;
  action_url?: string;
  is_read: boolean;
  created_at: string;
  sender_username?: string;
  sender_name?: string;
  sender_avatar?: string;
}

export interface Community {
  id: string;
  name: string;
  slug: string;
  description?: string;
  avatar_url?: string;
  banner_url?: string;
  category?: string;
  tags?: string[];
  is_private: boolean;
  is_verified: boolean;
  requires_approval: boolean;
  members_count: number;
  posts_count: number;
  is_member?: boolean;
  my_role?: string;
  created_at: string;
}

export interface LiveStream {
  id: string;
  user_id: string;
  title: string;
  thumbnail_url?: string;
  playback_url?: string;
  status: 'idle' | 'live' | 'ended';
  peak_viewers: number;
  total_viewers: number;
  started_at?: string;
  username: string;
  full_name: string;
  avatar_url?: string;
  is_verified: boolean;
}

export interface Wallet {
  id: string;
  user_id: string;
  balance_tokens: number;
  balance_usd: number;
  total_earned: number;
  total_spent: number;
  wallet_address?: string;
}

export interface PaginatedResponse<T> {
  items: T[];
  next_cursor: string | null;
  has_more: boolean;
}
