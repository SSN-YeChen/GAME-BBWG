import crypto from 'node:crypto';
import type express from 'express';

export type UserRole = 'admin' | 'temp';
export type SessionRecord = { username: string; role: UserRole; expiresAt: number };

const SESSION_COOKIE_NAME = 'bbwg_session';
const SESSION_TTL_MS = 1000 * 60 * 60 * 12;

function parseCookies(cookieHeader?: string): Record<string, string> {
  if (!cookieHeader) {
    return {};
  }

  const cookieMap: Record<string, string> = {};
  for (const part of cookieHeader.split(';')) {
    const trimmed = part.trim();
    if (!trimmed) {
      continue;
    }

    const separatorIndex = trimmed.indexOf('=');
    if (separatorIndex === -1) {
      continue;
    }

    const key = trimmed.slice(0, separatorIndex).trim();
    const value = trimmed.slice(separatorIndex + 1).trim();
    cookieMap[key] = decodeURIComponent(value);
  }

  return cookieMap;
}

function base64UrlEncode(value: Buffer | string): string {
  return Buffer.from(value)
    .toString('base64')
    .replaceAll('+', '-')
    .replaceAll('/', '_')
    .replaceAll('=', '');
}

function safeCompare(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);

  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }

  return crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

export class AuthService {
  private readonly sessions = new Map<string, SessionRecord>();

  constructor(
    private readonly options: {
      adminUsername: string;
      adminPassword: string;
      tempUsername: string;
      tempPassword: string;
      sessionSecret: string;
    }
  ) {}

  getSessionFromRequest(req: express.Request): SessionRecord | null {
    this.clearExpiredSessions();
    const cookies = parseCookies(req.headers.cookie);
    const token = cookies[SESSION_COOKIE_NAME];

    if (!token) {
      return null;
    }

    const session = this.sessions.get(token);
    if (!session) {
      return null;
    }

    if (session.expiresAt <= Date.now()) {
      this.sessions.delete(token);
      return null;
    }

    return session;
  }

  createSession(username: string, role: UserRole): string {
    const token = this.createSessionToken(username);
    this.sessions.set(token, {
      username,
      role,
      expiresAt: Date.now() + SESSION_TTL_MS
    });
    return token;
  }

  setSessionCookie(res: express.Response, token: string): void {
    const maxAgeSeconds = Math.floor(SESSION_TTL_MS / 1000);
    res.setHeader(
      'Set-Cookie',
      `${SESSION_COOKIE_NAME}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAgeSeconds}`
    );
  }

  clearSessionCookie(res: express.Response): void {
    res.setHeader(
      'Set-Cookie',
      `${SESSION_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`
    );
  }

  clearSessionFromRequest(req: express.Request): void {
    const cookies = parseCookies(req.headers.cookie);
    const token = cookies[SESSION_COOKIE_NAME];
    if (token) {
      this.sessions.delete(token);
    }
  }

  verifyCredentials(username: string, password: string): UserRole | null {
    if (
      this.options.adminUsername &&
      username === this.options.adminUsername &&
      safeCompare(password, this.options.adminPassword)
    ) {
      return 'admin';
    }

    if (
      this.options.tempUsername &&
      username === this.options.tempUsername &&
      safeCompare(password, this.options.tempPassword)
    ) {
      return 'temp';
    }

    return null;
  }

  requireAuth = (req: express.Request, res: express.Response, next: express.NextFunction): void => {
    const session = this.getSessionFromRequest(req);
    if (!session) {
      res.status(401).json({
        ok: false,
        error: '未登录或登录已过期'
      });
      return;
    }

    next();
  };

  requireRole(role: UserRole) {
    return (req: express.Request, res: express.Response, next: express.NextFunction): void => {
      const session = this.getSessionFromRequest(req);
      if (!session) {
        res.status(401).json({
          ok: false,
          error: '未登录或登录已过期'
        });
        return;
      }

      if (session.role !== role) {
        res.status(403).json({
          ok: false,
          error: '当前账号无权限执行该操作'
        });
        return;
      }

      next();
    };
  }

  private createSessionToken(username: string): string {
    const payload = `${username}:${crypto.randomUUID()}`;
    const signature = crypto.createHmac('sha256', this.options.sessionSecret).update(payload).digest();
    return `${base64UrlEncode(payload)}.${base64UrlEncode(signature)}`;
  }

  private clearExpiredSessions(): void {
    const now = Date.now();
    for (const [token, session] of this.sessions.entries()) {
      if (session.expiresAt <= now) {
        this.sessions.delete(token);
      }
    }
  }
}
