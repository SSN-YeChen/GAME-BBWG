import 'dotenv/config';
import crypto from 'node:crypto';
import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  cleanupVisitorLogs,
  createAccountsBatch,
  createVisitorLog,
  deleteAllVisitorLogs,
  deleteAccount,
  deleteAllAccounts,
  deleteBlacklistEntry,
  getBlacklistEntry,
  getDb,
  getExistingAccountIds,
  listAccounts,
  listBlacklistedAccounts,
  listBlacklistEntries,
  listVisitorLogs,
  reorderAccounts,
  setAccountBlacklist,
  upsertBlacklistEntry
} from './db.js';
import { getRedeemConfig, setRedeemToken } from './config.js';
import { fetchPlayerProfile, waitForNextAccount } from './player.js';
import { RedeemService, type RedeemProgressPayload } from './redeem.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const redeemService = new RedeemService();
const sseClients = new Set<express.Response>();
const importSseClients = new Set<express.Response>();
type UserRole = 'admin' | 'temp';
type SessionRecord = { username: string; role: UserRole; expiresAt: number };
const sessions = new Map<string, SessionRecord>();
const SESSION_COOKIE_NAME = 'bbwg_session';
const SESSION_TTL_MS = 1000 * 60 * 60 * 12;
const VISITOR_LOG_RETENTION_DAYS = 30;
const VISITOR_LOG_CLEANUP_INTERVAL_MS = 1000 * 60 * 60 * 24;
const adminUsername = process.env.ADMIN_USERNAME?.trim() || '';
const adminPassword = process.env.ADMIN_PASSWORD?.trim() || '';
const tempUsername = process.env.TEMP_USERNAME?.trim() || '';
const tempPassword = process.env.TEMP_PASSWORD?.trim() || '';
const sessionSecret = process.env.SESSION_SECRET?.trim() || 'bbwg-dev-session-secret';

app.set('trust proxy', true);
app.use(express.json());

function normalizeIpAddress(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return '';
  }
  if (trimmed.startsWith('::ffff:')) {
    return trimmed.slice(7);
  }
  return trimmed;
}

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

function createSessionToken(username: string): string {
  const payload = `${username}:${crypto.randomUUID()}`;
  const signature = crypto.createHmac('sha256', sessionSecret).update(payload).digest();
  return `${base64UrlEncode(payload)}.${base64UrlEncode(signature)}`;
}

function safeCompare(left: string, right: string): boolean {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);

  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }

  return crypto.timingSafeEqual(leftBuffer, rightBuffer);
}

function clearExpiredSessions(): void {
  const now = Date.now();
  for (const [token, session] of sessions.entries()) {
    if (session.expiresAt <= now) {
      sessions.delete(token);
    }
  }
}

function getSessionFromRequest(req: express.Request): SessionRecord | null {
  clearExpiredSessions();
  const cookies = parseCookies(req.headers.cookie);
  const token = cookies[SESSION_COOKIE_NAME];

  if (!token) {
    return null;
  }

  const session = sessions.get(token);
  if (!session) {
    return null;
  }

  if (session.expiresAt <= Date.now()) {
    sessions.delete(token);
    return null;
  }

  return session;
}

function setSessionCookie(res: express.Response, token: string): void {
  const maxAgeSeconds = Math.floor(SESSION_TTL_MS / 1000);
  res.setHeader(
    'Set-Cookie',
    `${SESSION_COOKIE_NAME}=${encodeURIComponent(token)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAgeSeconds}`
  );
}

function clearSessionCookie(res: express.Response): void {
  res.setHeader(
    'Set-Cookie',
    `${SESSION_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0`
  );
}

function requireAuth(req: express.Request, res: express.Response, next: express.NextFunction): void {
  const session = getSessionFromRequest(req);
  if (!session) {
    res.status(401).json({
      ok: false,
      error: '未登录或登录已过期'
    });
    return;
  }

  next();
}

function requireRole(role: UserRole) {
  return (req: express.Request, res: express.Response, next: express.NextFunction): void => {
    const session = getSessionFromRequest(req);
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

function sendJsonError(res: express.Response, error: unknown, status = 500): void {
  res.status(status).json({
    ok: false,
    error: error instanceof Error ? error.message : '未知错误'
  });
}

function broadcastRedeemProgress(payload: RedeemProgressPayload): void {
  const body = `data: ${JSON.stringify(payload)}\n\n`;
  for (const client of sseClients) {
    client.write(body);
  }
}

function broadcastImportProgress(payload: {
  type: 'start' | 'progress' | 'done';
  total?: number;
  processed?: number;
  inserted?: number;
  skipped?: number;
  failed?: number;
  accountId?: string;
}): void {
  const body = `data: ${JSON.stringify(payload)}\n\n`;
  for (const client of importSseClients) {
    client.write(body);
  }
}

app.use((req, res, next) => {
  const startedAt = Date.now();
  const shouldAudit = shouldAuditRequest(req);

  res.on('finish', () => {
    if (!shouldAudit) {
      return;
    }

    const session = getSessionFromRequest(req);
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
});

app.use(async (req, res, next) => {
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
});

app.use(express.static(path.resolve(__dirname, '../public')));

redeemService.on('progress', (payload: RedeemProgressPayload) => {
  broadcastRedeemProgress(payload);
});

app.get('/api/auth/status', (req, res) => {
  const session = getSessionFromRequest(req);
  res.json({
    authenticated: Boolean(session),
    username: session?.username ?? '',
    role: session?.role ?? ''
  });
});

app.post('/api/auth/login', (req, res) => {
  const { username, password } = req.body as { username?: string; password?: string };
  const normalizedUsername = username?.trim() ?? '';
  const normalizedPassword = password?.trim() ?? '';

  if (!adminUsername || !adminPassword) {
    sendJsonError(res, new Error('服务端未配置登录账号或密码。请先设置 .env。'), 500);
    return;
  }

  let matchedRole: UserRole | null = null;
  if (safeCompare(normalizedUsername, adminUsername) && safeCompare(normalizedPassword, adminPassword)) {
    matchedRole = 'admin';
  } else if (
    tempUsername &&
    tempPassword &&
    safeCompare(normalizedUsername, tempUsername) &&
    safeCompare(normalizedPassword, tempPassword)
  ) {
    matchedRole = 'temp';
  }

  if (!matchedRole) {
    sendJsonError(res, new Error('账号或密码错误。'), 401);
    return;
  }

  const token = createSessionToken(normalizedUsername);
  sessions.set(token, {
    username: normalizedUsername,
    role: matchedRole,
    expiresAt: Date.now() + SESSION_TTL_MS
  });
  setSessionCookie(res, token);

  res.json({
    ok: true,
    username: normalizedUsername,
    role: matchedRole
  });
});

app.post('/api/auth/logout', (req, res) => {
  const cookies = parseCookies(req.headers.cookie);
  const token = cookies[SESSION_COOKIE_NAME];

  if (token) {
    sessions.delete(token);
  }

  clearSessionCookie(res);
  res.json({ ok: true });
});

app.get('/api/redeem/events', requireAuth, (_req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  res.write(`data: ${JSON.stringify({ type: 'log', level: 'info', message: '已连接进度通道。' })}\n\n`);
  sseClients.add(res);

  const heartbeat = setInterval(() => {
    res.write(': ping\n\n');
  }, 15_000);

  res.on('close', () => {
    clearInterval(heartbeat);
    sseClients.delete(res);
  });
});

app.get('/api/accounts/import-events', requireAuth, (_req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  importSseClients.add(res);

  const heartbeat = setInterval(() => {
    res.write(': ping\n\n');
  }, 15_000);

  res.on('close', () => {
    clearInterval(heartbeat);
    importSseClients.delete(res);
  });
});

app.post('/api/accounts/batch', requireAuth, async (req, res) => {
  try {
    const { accountIds } = req.body as { accountIds?: string[] };
    const normalizedIds = Array.from(
      new Set((Array.isArray(accountIds) ? accountIds : []).map((item) => item.trim()).filter(Boolean))
    );
    const existingIds = await getExistingAccountIds(normalizedIds);
    const newIds = normalizedIds.filter((accountId) => !existingIds.has(accountId));
    const accountsToInsert: Array<{ accountId: string; name: string; details: Record<string, unknown> }> = [];
    let failed = 0;
    let processed = 0;

    broadcastImportProgress({
      type: 'start',
      total: normalizedIds.length,
      processed: 0,
      inserted: 0,
      skipped: normalizedIds.length - newIds.length,
      failed: 0
    });

    for (let index = 0; index < newIds.length; index += 1) {
      const accountId = newIds[index];

      try {
        const profile = await fetchPlayerProfile(accountId);
        accountsToInsert.push({
          accountId,
          name: profile.name,
          details: profile.details
        });
      } catch {
        failed += 1;
      }

      processed += 1;
      broadcastImportProgress({
        type: 'progress',
        total: normalizedIds.length,
        processed: processed + (normalizedIds.length - newIds.length),
        inserted: accountsToInsert.length,
        skipped: normalizedIds.length - newIds.length,
        failed,
        accountId
      });

      if (index < newIds.length - 1) {
        await waitForNextAccount();
      }
    }

    const result = await createAccountsBatch(accountsToInsert);
    broadcastImportProgress({
      type: 'done',
      total: normalizedIds.length,
      processed: normalizedIds.length,
      inserted: result.inserted,
      skipped: normalizedIds.length - newIds.length,
      failed
    });
    res.json({
      inserted: result.inserted,
      skipped: normalizedIds.length - newIds.length,
      failed
    });
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.get('/api/accounts', requireAuth, async (_req, res) => {
  try {
    const rows = await listAccounts();
    res.json(rows);
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.get('/api/accounts/blacklist', requireRole('admin'), async (_req, res) => {
  try {
    const rows = await listBlacklistedAccounts();
    res.json(rows);
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.post('/api/accounts/:accountId/blacklist', requireRole('admin'), async (req, res) => {
  try {
    const accountId = Array.isArray(req.params.accountId) ? req.params.accountId[0] : req.params.accountId;
    const updated = await setAccountBlacklist(accountId, true);
    if (!updated) {
      sendJsonError(res, new Error('账号不存在'), 404);
      return;
    }
    res.json({ ok: true });
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.delete('/api/accounts/:accountId/blacklist', requireRole('admin'), async (req, res) => {
  try {
    const accountId = Array.isArray(req.params.accountId) ? req.params.accountId[0] : req.params.accountId;
    const updated = await setAccountBlacklist(accountId, false);
    if (!updated) {
      sendJsonError(res, new Error('账号不存在'), 404);
      return;
    }
    res.json({ ok: true });
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.delete('/api/accounts/:accountId', requireRole('admin'), async (req, res) => {
  try {
    const accountId = Array.isArray(req.params.accountId) ? req.params.accountId[0] : req.params.accountId;
    await deleteAccount(accountId);
    res.json({ ok: true });
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.delete('/api/accounts', requireRole('admin'), async (_req, res) => {
  try {
    const deleted = await deleteAllAccounts();
    res.json({ deleted });
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.post('/api/accounts/reorder', requireRole('admin'), async (req, res) => {
  try {
    const { accountIds } = req.body as { accountIds?: string[] };
    await reorderAccounts(Array.isArray(accountIds) ? accountIds : []);
    res.json({ ok: true });
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.post('/api/redeem/run', requireRole('admin'), async (req, res) => {
  try {
    const { giftCode } = req.body as { giftCode?: string };
    const result = await redeemService.runBatchRedeem(giftCode ?? '');
    res.json({ ok: true, data: result });
  } catch (error) {
    res.json({
      ok: false,
      error: error instanceof Error ? error.message : '未知错误'
    });
  }
});

app.post('/api/redeem/retry-failed', requireRole('admin'), async (req, res) => {
  try {
    const { giftCode, accountIds } = req.body as { giftCode?: string; accountIds?: string[] };
    const result = await redeemService.runBatchRedeem(giftCode ?? '', Array.isArray(accountIds) ? accountIds : []);
    res.json({ ok: true, data: result });
  } catch (error) {
    res.json({
      ok: false,
      error: error instanceof Error ? error.message : '未知错误'
    });
  }
});

app.post('/api/redeem/stop', requireRole('admin'), (_req, res) => {
  res.json({
    ok: true,
    stopped: redeemService.requestCancel()
  });
});

app.post('/api/redeem/force-complete-all', requireRole('admin'), async (_req, res) => {
  try {
    const result = await redeemService.forceCompleteAllRedeem();
    res.json(result);
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.get('/api/config/redeem', requireAuth, (_req, res) => {
  res.json(getRedeemConfig());
});

app.post('/api/config/redeem-token', requireRole('admin'), (req, res) => {
  try {
    const { token } = req.body as { token?: string };
    setRedeemToken(token ?? '');
    res.json(getRedeemConfig());
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.get('/api/visitor-logs', requireRole('admin'), async (req, res) => {
  try {
    const rawLimit = Array.isArray(req.query.limit) ? req.query.limit[0] : req.query.limit;
    const parsedLimit = Number(rawLimit);
    const limit = Number.isFinite(parsedLimit) ? Math.max(1, Math.min(parsedLimit, 200)) : 100;
    const rows = await listVisitorLogs(limit);
    res.json({
      retentionDays: VISITOR_LOG_RETENTION_DAYS,
      items: rows
    });
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.delete('/api/visitor-logs', requireRole('admin'), async (_req, res) => {
  try {
    const deleted = await deleteAllVisitorLogs();
    res.json({ ok: true, deleted });
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.get('/api/visitor-blacklist', requireRole('admin'), async (_req, res) => {
  try {
    const items = await listBlacklistEntries();
    res.json(items);
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.post('/api/visitor-blacklist', requireRole('admin'), async (req, res) => {
  try {
    const { ipAddress, reason } = req.body as { ipAddress?: string; reason?: string };
    const normalizedIpAddress = normalizeIpAddress(ipAddress ?? '');
    const normalizedReason = reason?.trim() ?? '';

    if (!normalizedIpAddress) {
      sendJsonError(res, new Error('请输入要拉黑的 IP 地址。'), 400);
      return;
    }

    await upsertBlacklistEntry(normalizedIpAddress, normalizedReason);
    res.json({ ok: true });
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.delete('/api/visitor-blacklist/:ipAddress', requireRole('admin'), async (req, res) => {
  try {
    const ipAddress = normalizeIpAddress(
      Array.isArray(req.params.ipAddress) ? req.params.ipAddress[0] : req.params.ipAddress
    );
    await deleteBlacklistEntry(ipAddress);
    res.json({ ok: true });
  } catch (error) {
    sendJsonError(res, error);
  }
});

app.get('*', (_req, res) => {
  res.sendFile(path.resolve(__dirname, '../public/index.html'));
});

const port = Number(process.env.PORT || 3458);

void getDb().then(() => {
  void cleanupVisitorLogs(VISITOR_LOG_RETENTION_DAYS).catch((error: unknown) => {
    // eslint-disable-next-line no-console
    console.error('failed to cleanup visitor logs on startup', error);
  });
  setInterval(() => {
    void cleanupVisitorLogs(VISITOR_LOG_RETENTION_DAYS).catch((error: unknown) => {
      // eslint-disable-next-line no-console
      console.error('failed to cleanup visitor logs', error);
    });
  }, VISITOR_LOG_CLEANUP_INTERVAL_MS);

  app.listen(port, () => {
    // eslint-disable-next-line no-console
    console.log(`bb-web is running at http://localhost:${port}`);
  });
});
