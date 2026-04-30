import type express from 'express';
import { createVisitorLog, getBlacklistEntry } from '../core/visitorRepository.js';
import type { AuthService } from './auth.js';
import { normalizeIpAddress, sendJsonError } from './http.js';

function getClientIp(req: express.Request): string {
  const forwardedFor = req.headers['x-forwarded-for'];
  const forwardedIp = Array.isArray(forwardedFor) ? forwardedFor[0] : forwardedFor?.split(',')[0]?.trim();
  const cloudflareIp = typeof req.headers['cf-connecting-ip'] === 'string' ? req.headers['cf-connecting-ip'] : '';
  return normalizeIpAddress(cloudflareIp || forwardedIp || req.ip || '');
}

function getRequestProtocol(req: express.Request): string {
  const forwardedProto = req.headers['x-forwarded-proto'];
  if (Array.isArray(forwardedProto)) {
    return forwardedProto[0] || req.protocol;
  }
  return forwardedProto || req.protocol;
}

function getRequestHost(req: express.Request): string {
  const forwardedHost = req.headers['x-forwarded-host'];
  if (Array.isArray(forwardedHost)) {
    return forwardedHost[0] || req.get('host') || '';
  }
  return forwardedHost || req.get('host') || '';
}

function stringifyForLog(value: unknown, maxLength = 16_000): string {
  if (value === undefined) {
    return '';
  }

  try {
    const serialized = typeof value === 'string' ? value : JSON.stringify(value);
    return serialized.length > maxLength ? `${serialized.slice(0, maxLength)}...[truncated]` : serialized;
  } catch {
    return '[unserializable]';
  }
}

function shouldAuditRequest(req: express.Request): boolean {
  const requestPath = req.path || '';
  const staticAssetPattern =
    /^\/(?:app\.js|style\.css|favicon\.ico|manifest\.json|robots\.txt|apple-touch-icon.*|.*\.(?:js|css|map|png|jpg|jpeg|gif|svg|webp|ico|txt|woff|woff2))$/i;
  const auditExcludedPaths = new Set([
    '/',
    '/api/auth/status',
    '/api/config/redeem',
    '/api/visitor-logs',
    '/api/visitor-blacklist'
  ]);

  if (staticAssetPattern.test(requestPath)) {
    return false;
  }

  if (auditExcludedPaths.has(requestPath) || requestPath.startsWith('/api/visitor-blacklist/')) {
    return false;
  }

  return true;
}

export function createVisitorAuditMiddleware(authService: AuthService): express.RequestHandler {
  return (req, res, next) => {
    const startedAt = Date.now();
    const shouldAudit = shouldAuditRequest(req);

    res.on('finish', () => {
      if (!shouldAudit) {
        return;
      }

      const session = authService.getSessionFromRequest(req);
      void createVisitorLog({
        ipAddress: getClientIp(req),
        method: req.method,
        protocol: getRequestProtocol(req),
        host: getRequestHost(req),
        path: req.path,
        query: stringifyForLog(req.query),
        params: stringifyForLog(req.params),
        headers: stringifyForLog(req.headers),
        body: stringifyForLog(req.body),
        statusCode: res.statusCode,
        durationMs: Date.now() - startedAt,
        username: session?.username ?? '',
        userRole: session?.role ?? '',
        userAgent: req.get('user-agent') || '',
        referer: req.get('referer') || '',
        cfRay: req.get('cf-ray') || '',
        cfCountry: req.get('cf-ipcountry') || '',
        blocked: Boolean(res.locals.auditBlocked),
        blockReason: typeof res.locals.auditBlockReason === 'string' ? res.locals.auditBlockReason : '',
        createdAt: Date.now()
      }).catch((error: unknown) => {
        // eslint-disable-next-line no-console
        console.error('failed to persist visitor log', error);
      });
    });

    next();
  };
}

export function createVisitorBlacklistMiddleware(): express.RequestHandler {
  return async (req, res, next) => {
    try {
      const ipAddress = getClientIp(req);
      if (!ipAddress) {
        next();
        return;
      }

      const blacklistEntry = await getBlacklistEntry(ipAddress);
      if (!blacklistEntry) {
        next();
        return;
      }

      res.locals.auditBlocked = true;
      res.locals.auditBlockReason = blacklistEntry.reason || '命中访问黑名单';
      res.status(404).send('Not Found');
    } catch (error) {
      sendJsonError(res, error);
    }
  };
}
