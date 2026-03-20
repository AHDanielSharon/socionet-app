export function PostSkeleton() {
  return (
    <div className="bg-surface border border-border rounded-2xl overflow-hidden">
      <div className="p-4 flex gap-3">
        <div className="skeleton w-10 h-10 rounded-full" />
        <div className="flex-1 space-y-2">
          <div className="skeleton h-3.5 w-32 rounded" />
          <div className="skeleton h-2.5 w-20 rounded" />
        </div>
      </div>
      <div className="px-4 pb-3 space-y-2">
        <div className="skeleton h-3 w-full rounded" />
        <div className="skeleton h-3 w-4/5 rounded" />
      </div>
      <div className="skeleton h-56 w-full" />
      <div className="flex px-3 py-2.5 gap-1">
        {[1,2,3,4,5].map(i => <div key={i} className="skeleton h-8 flex-1 rounded-lg" />)}
      </div>
    </div>
  );
}
