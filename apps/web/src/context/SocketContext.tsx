'use client';
import { createContext, useContext, ReactNode } from 'react';
import { useSocketStore } from '@/lib/socket';
const SocketContext = createContext<any>(null);
export function SocketProvider({ children }: { children: ReactNode }) {
  const socket = useSocketStore();
  return <SocketContext.Provider value={socket}>{children}</SocketContext.Provider>;
}
export const useSocketContext = () => useContext(SocketContext);
