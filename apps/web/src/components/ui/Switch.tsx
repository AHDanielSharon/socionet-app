'use client';
import { cn } from '@/lib/utils';
export function Switch({ checked, onChange, label, description }: { checked: boolean; onChange: (v: boolean) => void; label?: string; description?: string }) {
  return (
    <div className="flex items-center justify-between">
      {(label || description) && <div className="flex-1 mr-4"><p className="text-sm font-medium">{label}</p>{description && <p className="text-xs text-text-3 mt-0.5">{description}</p>}</div>}
      <button onClick={() => onChange(!checked)} className={cn('relative w-11 h-6 rounded-full transition-colors flex-shrink-0', checked ? 'bg-accent' : 'bg-surface-3')}>
        <span className={cn('absolute top-0.5 w-5 h-5 bg-white rounded-full shadow transition-transform', checked ? 'translate-x-5' : 'translate-x-0.5')}/>
      </button>
    </div>
  );
}
