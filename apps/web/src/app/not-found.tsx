import Link from 'next/link';
export default function NotFound() {
  return (
    <div className="min-h-screen bg-bg flex flex-col items-center justify-center text-center p-8">
      <div className="text-9xl font-display font-black bg-gradient-to-r from-accent to-neon bg-clip-text text-transparent mb-4">404</div>
      <h2 className="font-display font-bold text-2xl mb-2">Page Not Found</h2>
      <p className="text-text-3 mb-8">This page does not exist on SOCIONET</p>
      <Link href="/" className="btn-primary">← Back to Home</Link>
    </div>
  );
}
