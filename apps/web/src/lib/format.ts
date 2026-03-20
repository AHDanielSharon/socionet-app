export const formatCurrency = (n: number, currency = 'USD') => new Intl.NumberFormat('en-US', { style: 'currency', currency }).format(n);
export const formatNumber = (n: number) => new Intl.NumberFormat('en-US').format(n);
export const formatPercent = (n: number) => `${(n * 100).toFixed(1)}%`;
export const pluralize = (n: number, word: string) => n === 1 ? `${n} ${word}` : `${n} ${word}s`;
export const capitalize = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);
