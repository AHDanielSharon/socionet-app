export const isValidEmail = (s: string) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s);
export const isValidUsername = (s: string) => /^[a-zA-Z0-9_.]{3,30}$/.test(s);
export const isValidPassword = (s: string) => s.length >= 8;
export const isValidPhone = (s: string) => /^\+?[1-9]\d{7,14}$/.test(s);
export const isValidUrl = (s: string) => { try { new URL(s); return true; } catch { return false; } };
