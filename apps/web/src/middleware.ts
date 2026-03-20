import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';
const PUBLIC = ['/auth', '/_next', '/favicon', '/icons', '/api'];
export function middleware(req: NextRequest) {
  const { pathname } = req.nextUrl;
  const isPublic = PUBLIC.some(p => pathname.startsWith(p));
  const token = req.cookies.get('socionet-token')?.value;
  if (!isPublic && !token) return NextResponse.redirect(new URL('/auth/login', req.url));
  if (pathname.startsWith('/auth/login') && token) return NextResponse.redirect(new URL('/', req.url));
  return NextResponse.next();
}
export const config = { matcher: ['/((?!_next/static|_next/image|favicon.ico).*)'] };
