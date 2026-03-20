import { create } from 'zustand';
interface NotifStore { unreadCount: number; setUnreadCount: (n: number) => void; increment: () => void; reset: () => void; }
export const useNotifStore = create<NotifStore>((set) => ({ unreadCount: 0, setUnreadCount: (n) => set({ unreadCount: n }), increment: () => set(s => ({ unreadCount: s.unreadCount + 1 })), reset: () => set({ unreadCount: 0 }) }));
