'use client';
import { ReactNode } from 'react';
import { cn } from '@/lib/utils';
interface Tab { id: string; label: string; icon?: ReactNode }
interface TabsProps { tabs: Tab[]; active: string; onChange: (id: string) => void; variant?: 'pills' | 'underline' }
export function Tabs({ tabs, active, onChange, variant = 'pills' }: TabsProps) {
  if (variant === 'underline') return (
    <div className="flex border-b border-border">
      {tabs.map(t => <button key={t.id} onClick={() => onChange(t.id)} className={cn('flex items-center gap-2 px-4 py-3 text-sm font-semibold border-b-2 -mb-px transition-all', active === t.id ? 'border-accent text-accent' : 'border-transparent text-text-3 hover:text-text')}>{t.icon}{t.label}</button>)}
    </div>
  );
  return (
    <div className="flex gap-1 bg-surface p-1 rounded-xl">
      {tabs.map(t => <button key={t.id} onClick={() => onChange(t.id)} className={cn('flex items-center gap-2 flex-1 justify-center py-2 rounded-lg text-sm font-semibold transition-all', active === t.id ? 'bg-surface-2 text-text shadow-card' : 'text-text-3 hover:text-text')}>{t.icon}{t.label}</button>)}
    </div>
  );
}
