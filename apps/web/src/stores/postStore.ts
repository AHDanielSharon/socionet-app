import { create } from 'zustand';

interface PostStore {
  draftCaption: string;
  draftMedia: File[];
  setDraftCaption: (caption: string) => void;
  addDraftMedia: (files: File[]) => void;
  clearDraft: () => void;
  likedPosts: Set<string>;
  savedPosts: Set<string>;
  setLiked: (postId: string, liked: boolean) => void;
  setSaved: (postId: string, saved: boolean) => void;
}

export const usePostStore = create<PostStore>((set) => ({
  draftCaption: '',
  draftMedia: [],
  likedPosts: new Set(),
  savedPosts: new Set(),
  setDraftCaption: (caption) => set({ draftCaption: caption }),
  addDraftMedia: (files) => set((s) => ({ draftMedia: [...s.draftMedia, ...files] })),
  clearDraft: () => set({ draftCaption: '', draftMedia: [] }),
  setLiked: (postId, liked) => set((s) => {
    const next = new Set(s.likedPosts);
    liked ? next.add(postId) : next.delete(postId);
    return { likedPosts: next };
  }),
  setSaved: (postId, saved) => set((s) => {
    const next = new Set(s.savedPosts);
    saved ? next.add(postId) : next.delete(postId);
    return { savedPosts: next };
  }),
}));
