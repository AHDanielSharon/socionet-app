'use client';
import { useState, useEffect } from 'react';
import { useParams, useRouter } from 'next/navigation';
import AppLayout from '@/components/layout/AppLayout';
import { PostCard } from '@/components/feed/PostCard';
import { api } from '@/lib/api';
import { ArrowLeft } from 'lucide-react';

export default function PostPage() {
  const { id } = useParams();
  const router = useRouter();
  const [post, setPost] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (id) api.posts.get(id as string).then(r => setPost(r.data.post)).catch(() => {}).finally(() => setLoading(false));
  }, [id]);

  return (
    <AppLayout>
      <div className="max-w-xl mx-auto px-4 py-6">
        <button onClick={() => router.back()} className="flex items-center gap-2 text-text-2 hover:text-text mb-4 transition-colors">
          <ArrowLeft size={18}/> Back
        </button>
        {loading ? <div className="skeleton h-64 rounded-2xl"/> : post ? <PostCard post={post} onDelete={() => router.push('/')}/> : <div className="text-center py-20 text-text-3">Post not found</div>}
      </div>
    </AppLayout>
  );
}
