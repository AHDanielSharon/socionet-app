import { ReactNode } from 'react';
import { cn } from '@/lib/utils';
export function Card({ children, className, onClick }: { children: ReactNode; className?: string; onClick?: () => void }) {
  return <div className={cn('card', onClick && 'cursor-pointer', className)} onClick={onClick}>{children}</div>;
}
export function CardHeader({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn('flex items-center justify-between mb-4', className)}>{children}</div>;
}
export function CardTitle({ children }: { children: ReactNode }) {
  return <h3 className="font-display font-bold text-lg">{children}</h3>;
}
