export const isUUID = (s: string) => /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(s);
export const isEmail = (s: string) => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(s);
export const isUsername = (s: string) => /^[a-zA-Z0-9_.]{3,30}$/.test(s);
export const isPhone = (s: string) => /^\+?[1-9]\d{7,14}$/.test(s);
