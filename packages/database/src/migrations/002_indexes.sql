CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_posts_user_created ON posts(user_id, created_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_messages_conv_created ON messages(conversation_id, created_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_notifications_recipient ON notifications(recipient_id, is_read, created_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_relationships_follower ON relationships(follower_id, type, status);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_users_search ON users USING gin(to_tsvector('english', full_name || ' ' || username));
