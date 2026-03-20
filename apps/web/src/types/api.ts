export interface ApiError {
  error: string;
  code?: string;
  details?: Array<{ field: string; message: string }>;
}

export interface ApiResponse<T> {
  data: T;
  status: number;
}
