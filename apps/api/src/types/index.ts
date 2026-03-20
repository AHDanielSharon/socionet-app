export interface PaginationOptions { limit?: number; cursor?: string; offset?: number; }
export interface PaginatedResult<T> { items: T[]; next_cursor: string | null; has_more: boolean; }
export interface TokenPayload { sub: string; username: string; iat: number; exp: number; type: 'access' | 'refresh'; }
export interface MediaInfo { id: string; type: string; url: string; thumbnail_url?: string; width?: number; height?: number; size_bytes: number; }
export interface NotificationPayload { recipient_id: string; sender_id?: string; type: string; entity_type?: string; entity_id?: string; title?: string; body?: string; }
