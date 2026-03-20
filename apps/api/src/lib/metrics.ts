const counters: Record<string, number> = {};
export const metrics = {
  increment: (name: string, val = 1) => { counters[name] = (counters[name] || 0) + val; },
  getAll: () => counters,
};
export {};
