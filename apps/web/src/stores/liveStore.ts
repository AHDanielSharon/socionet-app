import { create } from 'zustand';
interface LiveStore { activeStream: any | null; viewerCount: number; setStream: (s: any) => void; setViewerCount: (n: number) => void; clearStream: () => void; }
export const useLiveStore = create<LiveStore>((set) => ({ activeStream: null, viewerCount: 0, setStream: (activeStream) => set({ activeStream }), setViewerCount: (viewerCount) => set({ viewerCount }), clearStream: () => set({ activeStream: null, viewerCount: 0 }) }));
