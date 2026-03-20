'use client';
import { createContext, useContext, ReactNode } from 'react';
import { useAuthStore } from '@/stores/authStore';
const AuthContext = createContext<any>(null);
export function AuthProvider({ children }: { children: ReactNode }) {
  const auth = useAuthStore();
  return <AuthContext.Provider value={auth}>{children}</AuthContext.Provider>;
}
export const useAuth = () => useContext(AuthContext);
