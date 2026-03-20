'use client';
import { useState } from 'react';
import { useParams, useRouter } from 'next/navigation';
import Image from 'next/image';
import { motion, AnimatePresence } from 'framer-motion';
import {
  BadgeCheck, Grid3X3, Video, Film, Bookmark, Heart,
  Link, MapPin, Calendar, MoreHorizontal, UserPlus, UserCheck,
  MessageCircle, Phone, Share2, Settings, Edit3
} from 'lucide-react';
import AppLayout from '@/components/layout/AppLayout';
import { Avatar } from '@/components/ui/Avatar';
import { PostCard } from '@/components/feed/PostCard';
import { PostSkeleton } from '@/components/feed/PostSkeleton';
import { useProfile } from '@/hooks/useProfile';
import { useAuthStore } from '@/stores/authStore';
import { api } from '@/lib/api';
import { formatCount, formatFullDate } from '@/lib/utils';
import toast from 'react-hot-toast';

const TABS = [
  { id: 'posts', label: 'Posts', icon: Grid3X3 },
  { id: 'reels', label: 'Reels', icon: Film },
  { id: 'videos', label: 'Videos', icon: Video },
  { id: 'saved', label: 'Saved', icon: Bookmark },
  { id: 'liked', label: 'Liked', icon: Heart },
];

export default function ProfilePage() {
  const params = useParams();
  const router = useRouter();
  const { user: me } = useAuthStore();
  const username = params?.username as string || me?.username || '';
  const { profile, loading, toggleFollow } = useProfile(username);
  const [activeTab, setActiveTab] = useState('posts');
  const [posts, setPosts] = useState<any[]>([]);
  const [postsLoaded, setPostsLoaded] = useState(false);
  const [postsLoading, setPostsLoading] = useState(false);

  const isMe = profile?.username === me?.username;

  const loadPosts = async (tab: string) => {
    setPostsLoading(true);
    try {
      const typeMap: Record<string, string> = { reels: 'reel', videos: 'video' };
      const res = tab === 'saved'
        ? await api.posts.getSaved()
        : await api.users.getPosts(username, { type: typeMap[tab], limit: 18 });
      setPosts(res.data.posts || []);
      setPostsLoaded(true);
    } catch {} finally { setPostsLoading(false); }
  };

  const handleTabChange = (tab: string) => {
    setActiveTab(tab);
    setPosts([]);
    setPostsLoaded(false);
    loadPosts(tab);
  };

  if (!postsLoaded && !postsLoading) loadPosts('posts');

  if (loading) {
    return (
      <AppLayout>
        <div className="animate-pulse">
          <div className="h-52 bg-surface-2" />
          <div className="px-6 py-4">
            <div className="skeleton w-24 h-24 rounded-full -mt-12" />
            <div className="mt-4 space-y-2">
              <div className="skeleton h-6 w-40 rounded" />
              <div className="skeleton h-4 w-24 rounded" />
            </div>
          </div>
        </div>
      </AppLayout>
    );
  }

  if (!profile) {
    return (
      <AppLayout>
        <div className="flex flex-col items-center justify-center min-h-[60vh] text-center px-4">
          <div className="text-5xl mb-4">👤</div>
          <h2 className="font-display font-bold text-2xl mb-2">User not found</h2>
          <p className="text-text-3 mb-6">@{username} doesn't exist on SOCIONET</p>
          <button onClick={() => router.back()} className="btn-ghost btn-sm">← Go back</button>
        </div>
      </AppLayout>
    );
  }

  const isFollowing = profile.is_following;
  const isPrivate = profile.is_private && !isFollowing && !isMe;

  return (
    <AppLayout>
      <div className="max-w-4xl mx-auto">
        {/* Banner */}
        <div className="relative h-52 bg-gradient-hero overflow-hidden">
          {profile.banner_url && (
            <Image src={profile.banner_url} alt="Banner" fill className="object-cover" />
          )}
          <div className="absolute inset-0 bg-gradient-to-b from-transparent to-bg/60" />
          {isMe && (
            <button className="absolute top-4 right-4 btn-ghost btn-xs bg-black/40 border-white/20 text-white hover:bg-black/60">
              <Edit3 size={12} /> Edit Cover
            </button>
          )}
        </div>

        {/* Profile info */}
        <div className="px-6 pb-6">
          {/* Avatar row */}
          <div className="flex items-end justify-between -mt-14 mb-4">
            <div className="relative">
              <div className="w-28 h-28 rounded-full overflow-hidden border-4 border-bg bg-surface shadow-card2">
                <Avatar src={profile.avatar_url} name={profile.full_name} size={112} />
              </div>
              {profile.is_verified && (
                <div className="absolute bottom-1 right-1 w-7 h-7 bg-accent rounded-full flex items-center justify-center border-2 border-bg">
                  <BadgeCheck size={14} className="text-white fill-white" />
                </div>
              )}
            </div>

            {/* Action buttons */}
            <div className="flex gap-2 mb-2">
              {isMe ? (
                <>
                  <button onClick={() => router.push('/settings')} className="btn-ghost btn-sm">
                    <Settings size={15} /> Settings
                  </button>
                  <button className="btn-primary btn-sm">
                    <Edit3 size={15} /> Edit Profile
                  </button>
                </>
              ) : (
                <>
                  <button
                    onClick={() => api.conversations.create({ user_ids: [profile.id] }).then(r => router.push(`/messages?conv=${r.data.conversation.id}`))}
                    className="btn-ghost btn-sm"
                  >
                    <MessageCircle size={15} />
                  </button>
                  <button className="btn-ghost btn-sm" onClick={() => {
                    navigator.clipboard.writeText(`${window.location.origin}/profile/${profile.username}`);
                    toast.success('Profile link copied!');
                  }}>
                    <Share2 size={15} />
                  </button>
                  <button
                    onClick={toggleFollow}
                    className={isFollowing ? 'btn-ghost btn-sm' : 'btn-primary btn-sm'}
                  >
                    {isFollowing ? (<><UserCheck size={15} /> Following</>) : (<><UserPlus size={15} /> Follow</>)}
                  </button>
                  {profile.follow_requested && !isFollowing && (
                    <span className="text-xs text-text-3 self-center">Requested</span>
                  )}
                </>
              )}
            </div>
          </div>

          {/* Name & handle */}
          <div className="mb-3">
            <div className="flex items-center gap-2 flex-wrap">
              <h1 className="font-display font-black text-2xl">{profile.full_name}</h1>
              {profile.is_verified && <BadgeCheck size={18} className="text-accent" />}
              {profile.is_creator && <span className="badge badge-neon text-[10px] py-0">Creator</span>}
              {profile.is_business && <span className="badge badge-accent text-[10px] py-0">Business</span>}
            </div>
            <p className="text-text-3 font-mono text-sm">@{profile.username}</p>
          </div>

          {/* Bio */}
          {profile.bio && (
            <p className="text-text-2 text-sm leading-relaxed mb-3 max-w-lg">{profile.bio}</p>
          )}

          {/* Meta */}
          <div className="flex flex-wrap gap-x-4 gap-y-1 mb-4 text-sm text-text-3">
            {profile.location && (
              <span className="flex items-center gap-1"><MapPin size={13} /> {profile.location}</span>
            )}
            {profile.website && (
              <a href={profile.website} target="_blank" rel="noopener noreferrer" className="flex items-center gap-1 text-accent-2 hover:underline">
                <Link size={13} /> {profile.website.replace(/^https?:\/\//, '')}
              </a>
            )}
            {profile.created_at && (
              <span className="flex items-center gap-1"><Calendar size={13} /> Joined {formatFullDate(profile.created_at)}</span>
            )}
          </div>

          {/* Stats */}
          <div className="flex gap-6">
            {[
              { label: 'Posts', value: profile.posts_count },
              { label: 'Followers', value: profile.followers_count, action: () => router.push(`/profile/${username}/followers`) },
              { label: 'Following', value: profile.following_count, action: () => router.push(`/profile/${username}/following`) },
            ].map(({ label, value, action }) => (
              <button
                key={label}
                onClick={action}
                className={`text-center ${action ? 'cursor-pointer hover:opacity-80' : ''} transition-opacity`}
              >
                <div className="font-display font-black text-xl">{formatCount(value)}</div>
                <div className="text-xs text-text-3">{label}</div>
              </button>
            ))}
          </div>
        </div>

        {/* Private account guard */}
        {isPrivate ? (
          <div className="flex flex-col items-center justify-center py-20 text-center px-4 border-t border-border">
            <div className="text-4xl mb-4">🔒</div>
            <h3 className="font-display font-bold text-xl mb-2">This account is private</h3>
            <p className="text-text-3 text-sm mb-6">Follow @{username} to see their posts</p>
            <button onClick={toggleFollow} className="btn-primary">
              <UserPlus size={16} /> {profile.follow_requested ? 'Requested' : 'Follow'}
            </button>
          </div>
        ) : (
          <>
            {/* Tabs */}
            <div className="border-t border-border">
              <div className="flex overflow-x-auto" style={{ scrollbarWidth: 'none' }}>
                {TABS.filter(t => isMe || !['saved', 'liked'].includes(t.id)).map(tab => {
                  const Icon = tab.icon;
                  return (
                    <button
                      key={tab.id}
                      onClick={() => handleTabChange(tab.id)}
                      className={`flex items-center gap-2 px-5 py-3.5 text-sm font-semibold border-b-2 transition-all whitespace-nowrap ${
                        activeTab === tab.id
                          ? 'border-accent text-accent'
                          : 'border-transparent text-text-3 hover:text-text-2'
                      }`}
                    >
                      <Icon size={15} />
                      {tab.label}
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Content */}
            <div className="p-4">
              {postsLoading ? (
                activeTab === 'posts' ? (
                  <div className="space-y-4">{Array.from({ length: 3 }).map((_, i) => <PostSkeleton key={i} />)}</div>
                ) : (
                  <div className="grid grid-cols-3 gap-0.5">
                    {Array.from({ length: 9 }).map((_, i) => <div key={i} className="skeleton aspect-square" />)}
                  </div>
                )
              ) : posts.length === 0 ? (
                <div className="text-center py-16">
                  <div className="text-5xl mb-3">📭</div>
                  <p className="text-text-3">No {activeTab} yet</p>
                </div>
              ) : activeTab === 'posts' ? (
                <div className="space-y-4 max-w-xl mx-auto">
                  {posts.map((p, i) => (
                    <motion.div key={p.id} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ delay: i * 0.04 }}>
                      <PostCard post={p} />
                    </motion.div>
                  ))}
                </div>
              ) : (
                <div className="grid grid-cols-3 gap-0.5">
                  {posts.map((p, i) => (
                    <motion.div
                      key={p.id}
                      className="relative aspect-square overflow-hidden bg-surface-2 cursor-pointer"
                      onClick={() => router.push(`/post/${p.id}`)}
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      transition={{ delay: i * 0.02 }}
                    >
                      {p.media?.[0] ? (
                        <Image src={p.media[0].url} alt="" fill className="object-cover" loading="lazy" />
                      ) : (
                        <div className="w-full h-full flex items-center justify-center bg-gradient-hero p-2">
                          <p className="text-xs text-text-2 text-center line-clamp-4">{p.caption}</p>
                        </div>
                      )}
                      <div className="absolute inset-0 bg-black/40 opacity-0 hover:opacity-100 transition-opacity flex items-center justify-center gap-3 text-white text-sm font-semibold">
                        <span>❤️ {formatCount(p.likes_count)}</span>
                        <span>💬 {formatCount(p.comments_count)}</span>
                      </div>
                    </motion.div>
                  ))}
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </AppLayout>
  );
}
