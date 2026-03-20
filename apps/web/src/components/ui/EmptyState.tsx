import { ReactNode } from 'react';
interface EmptyStateProps { icon?: ReactNode; emoji?: string; title: string; description?: string; action?: ReactNode }
export function EmptyState({ icon, emoji, title, description, action }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center text-center py-16 px-4">
      {emoji ? <div className="text-5xl mb-4">{emoji}</div> : icon && <div className="mb-4 text-text-4">{icon}</div>}
      <h3 className="font-display font-bold text-xl mb-2">{title}</h3>
      {description && <p className="text-text-3 text-sm max-w-xs mb-6">{description}</p>}
      {action}
    </div>
  );
}
