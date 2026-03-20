import { create } from 'zustand';

interface UIStore {
  theme: 'dark' | 'light' | 'midnight';
  sidebarCollapsed: boolean;
  createPostOpen: boolean;
  searchOpen: boolean;
  activeModal: string | null;
  setTheme: (theme: 'dark' | 'light' | 'midnight') => void;
  setSidebarCollapsed: (v: boolean) => void;
  setCreatePostOpen: (v: boolean) => void;
  setSearchOpen: (v: boolean) => void;
  openModal: (id: string) => void;
  closeModal: () => void;
}

export const useUIStore = create<UIStore>((set) => ({
  theme: 'dark',
  sidebarCollapsed: false,
  createPostOpen: false,
  searchOpen: false,
  activeModal: null,
  setTheme: (theme) => set({ theme }),
  setSidebarCollapsed: (v) => set({ sidebarCollapsed: v }),
  setCreatePostOpen: (v) => set({ createPostOpen: v }),
  setSearchOpen: (v) => set({ searchOpen: v }),
  openModal: (id) => set({ activeModal: id }),
  closeModal: () => set({ activeModal: null }),
}));
