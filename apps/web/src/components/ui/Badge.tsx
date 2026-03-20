import { cn } from '@/lib/utils';
import { ReactNode } from 'react';
type Variant = 'accent' | 'neon' | 'green' | 'red' | 'orange' | 'blue' | 'default';
const variants: Record<Variant, string> = {
  accent: 'bg-accent/20 text-accent-3 border-accent/30',
  neon: 'bg-neon/15 text-neon border-neon/25',
  green: 'bg-green-500/15 text-green-400 border-green-500/25',
  red: 'bg-red-500/15 text-red-400 border-red-500/25',
  orange: 'bg-orange-500/15 text-orange-400 border-orange-500/25',
  blue: 'bg-blue-500/15 text-blue-400 border-blue-500/25',
  default: 'bg-surface-2 text-text-2 border-border',
};
export function Badge({ children, variant = 'default', className }: { children: ReactNode; variant?: Variant; className?: string }) {
  return <span className={cn('inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-xs font-semibold border', variants[variant], className)}>{children}</span>;
}
