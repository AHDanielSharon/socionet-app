'use client';
export default function Error({ error, reset }: { error: Error; reset: () => void }) {
  return (
    <div className="min-h-screen bg-bg flex flex-col items-center justify-center text-center p-8">
      <div className="text-6xl mb-4">⚠️</div>
      <h2 className="font-display font-bold text-2xl mb-2">Something went wrong</h2>
      <p className="text-text-3 mb-6">{error.message}</p>
      <button onClick={reset} className="btn-primary">Try Again</button>
    </div>
  );
}
