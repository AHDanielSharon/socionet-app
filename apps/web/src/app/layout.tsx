import type { Metadata, Viewport } from 'next';
import { Syne, DM_Sans, JetBrains_Mono } from 'next/font/google';
import './globals.css';
import { Providers } from '@/components/layout/Providers';

const syne = Syne({ subsets:['latin'], weight:['400','500','600','700','800'], variable:'--font-syne', display:'swap' });
const dmSans = DM_Sans({ subsets:['latin'], weight:['300','400','500','600'], variable:'--font-dm-sans', display:'swap' });
const jetbrains = JetBrains_Mono({ subsets:['latin'], weight:['400','500','600'], variable:'--font-jetbrains', display:'swap' });

export const metadata: Metadata = {
  title: { default:'SOCIONET', template:'%s | SOCIONET' },
  description: 'The next-generation decentralized social platform. Private, powerful, yours.',
  keywords: ['social media','decentralized','private','messaging','communities','blockchain'],
  authors: [{ name:'SOCIONET' }],
};

export const viewport: Viewport = { themeColor:'#060608', width:'device-width', initialScale:1, maximumScale:1 };

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${syne.variable} ${dmSans.variable} ${jetbrains.variable}`} suppressHydrationWarning>
      <body className="bg-bg text-text font-sans antialiased"><Providers>{children}</Providers></body>
    </html>
  );
}
