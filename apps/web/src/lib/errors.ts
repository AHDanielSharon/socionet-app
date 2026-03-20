export class ApiError extends Error {
  status: number;
  code?: string;
  constructor(message: string, status: number, code?: string) {
    super(message);
    this.status = status;
    this.code = code;
    this.name = 'ApiError';
  }
}

export const getErrorMessage = (err: unknown): string => {
  if (err instanceof ApiError) return err.message;
  if (err instanceof Error) return err.message;
  if (typeof err === 'string') return err;
  return 'An unexpected error occurred';
};

export const isAuthError = (err: unknown): boolean => {
  return err instanceof ApiError && err.status === 401;
};

export const isNetworkError = (err: unknown): boolean => {
  return err instanceof TypeError && err.message.includes('fetch');
};
