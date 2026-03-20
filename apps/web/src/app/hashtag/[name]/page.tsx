'use client';
import { useState, useEffect } from 'react';
import { useParams } from 'next/navigation';
import Image from 'next/image';
import AppLayout from '@/components/layout/AppLayout';
import { api } from '@/lib/api';
import { formatCount } from '@/lib/utils';
import { Hash } from 'lucide-react';
import { useRouter } from 'next/navigation';

export default function HashtagPage() {
  const { name } = useParams();
  const router = useRouter();
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  useEffect(() => { if(name) api.search.hashtag(name as string).then(r=>setData(r.data)).catch(()=>{}).finally(()=>setLoading(false)); }, [name]);
  return (
    <AppLayout>
      <div className="max-w-3xl mx-auto px-4 py-6">
        {loading ? <div className="skeleton h-24 rounded-2xl mb-6"/> : data && (
          <div className="flex items-center gap-4 mb-6">
            <div className="w-16 h-16 rounded-full bg-gradient-accent flex items-center justify-center"><Hash size={28} className="text-white"/></div>
            <div><h1 className="font-display font-black text-2xl">#{data.hashtag?.name}</h1><p className="text-text-3">{formatCount(data.hashtag?.posts_count || 0)} posts</p></div>
          </div>
        )}
        <div className="grid grid-cols-3 gap-0.5 rounded-xl overflow-hidden">
          {(data?.posts || []).map((p: any) => (
            <div key={p.id} className="aspect-square bg-surface-2 cursor-pointer relative overflow-hidden" onClick={()=>router.push(`/post/${p.id}`)}>
              {p.first_media ? <Image src={p.first_media.url} alt="" fill className="object-cover"/> : <div className="p-2 flex items-center justify-center h-full"><p className="text-xs text-text-3 text-center line-clamp-3">{p.caption}</p></div>}
            </div>
          ))}
        </div>
      </div>
    </AppLayout>
  );
}
