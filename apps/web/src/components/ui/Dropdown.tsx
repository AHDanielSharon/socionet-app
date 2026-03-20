'use client';
import { useState, useRef, useEffect, ReactNode } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from '@/lib/utils';
interface DropdownItem { label: string; icon?: ReactNode; onClick: () => void; danger?: boolean }
export function Dropdown({ trigger, items, align = 'right' }: { trigger: ReactNode; items: DropdownItem[]; align?: 'left' | 'right' }) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const h = (e: MouseEvent) => { if (!ref.current?.contains(e.target as Node)) setOpen(false); };
    document.addEventListener('mousedown', h);
    return () => document.removeEventListener('mousedown', h);
  }, []);
  return (
    <div className="relative" ref={ref}>
      <div onClick={() => setOpen(!open)}>{trigger}</div>
      <AnimatePresence>
        {open && (
          <motion.div className={cn('absolute top-full mt-1.5 w-48 bg-surface-2 border border-border-2 rounded-xl shadow-card2 overflow-hidden z-50', align === 'right' ? 'right-0' : 'left-0')} initial={{opacity:0,scale:0.95,y:-5}} animate={{opacity:1,scale:1,y:0}} exit={{opacity:0,scale:0.95,y:-5}}>
            {items.map((item,i) => (
              <button key={i} onClick={() => { item.onClick(); setOpen(false); }} className={cn('w-full flex items-center gap-2.5 px-4 py-2.5 text-sm hover:bg-surface-3 transition-colors', item.danger ? 'text-red-400' : 'text-text-2')}>
                {item.icon}{item.label}
              </button>
            ))}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
