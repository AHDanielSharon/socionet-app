'use client';
import { ReactNode, useEffect } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { Toaster } from 'react-hot-toast';
import { useAuthStore } from '@/stores/authStore';
import { useSocket, useNotifStore } from '@/lib/socket';
import { api } from '@/lib/api';

const queryClient = new QueryClient({ defaultOptions: { queries: { staleTime: 30000, retry: 1, refetchOnWindowFocus: false } } });

function SocketInit({ children }: { children: ReactNode }) {
  const { user, isAuthenticated } = useAuthStore();
  useSocket(isAuthenticated ? user?.id || null : null);
  const { setUnreadCount } = useNotifStore();
  useEffect(() => {
    if (isAuthenticated) api.notifications.getUnreadCount().then(r => setUnreadCount(r.data.count)).catch(() => {});
  }, [isAuthenticated]);
  return <>{children}</>;
}

function AuthInit({ children }: { children: ReactNode }) {
  const { accessToken, fetchMe } = useAuthStore();
  useEffect(() => { if (accessToken) fetchMe(); }, []);
  return <>{children}</>;
}

export function Providers({ children }: { children: ReactNode }) {
  return (
    <QueryClientProvider client={queryClient}>
      <AuthInit>
        <SocketInit>
          {children}
          <Toaster position="bottom-center" toastOptions={{ duration: 3000, style: { background: '#1c1c28', color: '#f0f0fa', border: '1px solid #333348', borderRadius: '12px', fontFamily: 'var(--font-dm-sans)', fontSize: '14px', boxShadow: '0 8px 32px rgba(0,0,0,0.6)' }, success: { iconTheme: { primary: '#00e5a0', secondary: '#1c1c28' } }, error: { iconTheme: { primary: '#ff4d6d', secondary: '#1c1c28' } } }}/>
        </SocketInit>
      </AuthInit>
    </QueryClientProvider>
  );
}
