'use client';
import { ReactNode, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
export function Tooltip({ children, content, side = 'top' }: { children: ReactNode; content: string; side?: 'top'|'bottom'|'left'|'right' }) {
  const [show, setShow] = useState(false);
  const pos = { top:'bottom-full mb-2 left-1/2 -translate-x-1/2', bottom:'top-full mt-2 left-1/2 -translate-x-1/2', left:'right-full mr-2 top-1/2 -translate-y-1/2', right:'left-full ml-2 top-1/2 -translate-y-1/2' };
  return (
    <div className="relative inline-flex" onMouseEnter={()=>setShow(true)} onMouseLeave={()=>setShow(false)}>
      {children}
      <AnimatePresence>
        {show && <motion.div className={`absolute z-50 px-2 py-1 text-xs text-white bg-gray-900 rounded-lg whitespace-nowrap pointer-events-none ${pos[side]}`} initial={{opacity:0,scale:0.9}} animate={{opacity:1,scale:1}} exit={{opacity:0,scale:0.9}}>{content}</motion.div>}
      </AnimatePresence>
    </div>
  );
}
