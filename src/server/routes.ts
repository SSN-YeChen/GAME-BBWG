import type express from 'express';
import {
  createAccountsBatch,
  deleteAccount,
  deleteAllAccounts,
  deleteAllVisitorLogs,
  deleteBlacklistEntry,
  getExistingAccountIds,
  listAccounts,
  listBlacklistedAccounts,
  listBlacklistEntries,
  listRedeemCodes,
  listVisitorLogs,
  reorderAccounts,
  setAccountBlacklist,
  upsertBlacklistEntry
} from '../core/db.js';
import { getRedeemConfig, setRedeemToken } from '../core/config.js';
import { AutoRedeemCoordinator } from '../services/autoRedeem.js';
import { fetchPlayerProfile, waitForNextAccount } from '../services/player.js';
import { RedeemService } from '../services/redeem.js';
import { fetchRemoteRedeemToken } from '../services/redeemToken.js';
import { AuthService } from './auth.js';
import { normalizeIpAddress, sendJsonError } from './http.js';
import { SseHub } from './sse.js';

export function registerApiRoutes(options: {
  app: express.Express;
  authService: AuthService;
  redeemService: RedeemService;
  autoRedeemCoordinator: AutoRedeemCoordinator;
  sseHub: SseHub;
  pollActiveRedeemCodeSource: () => Promise<{ insertedCodes: string[] }>;
  visitorLogRetentionDays: number;
  hasAdminCredentials: boolean;
}): void {
  const {
    app,
    authService,
    redeemService,
    autoRedeemCoordinator,
    sseHub,
    pollActiveRedeemCodeSource,
    visitorLogRetentionDays,
    hasAdminCredentials
  } = options;
  const requireAuth = authService.requireAuth;
  const requireRole = authService.requireRole.bind(authService);

  app.get('/api/auth/status', (req, res) => {
    const session = authService.getSessionFromRequest(req);
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

    if (!hasAdminCredentials) {
      sendJsonError(res, new Error('服务端未配置登录账号或密码。请先设置 .env。'), 500);
      return;
    }

    const matchedRole = authService.verifyCredentials(normalizedUsername, normalizedPassword);
    if (!matchedRole) {
      sendJsonError(res, new Error('账号或密码错误。'), 401);
      return;
    }

    const token = authService.createSession(normalizedUsername, matchedRole);
    authService.setSessionCookie(res, token);

    res.json({
      ok: true,
      username: normalizedUsername,
      role: matchedRole
    });
  });

  app.post('/api/auth/logout', (req, res) => {
    authService.clearSessionFromRequest(req);
    authService.clearSessionCookie(res);
    res.json({ ok: true });
  });

  app.get('/api/redeem/events', requireAuth, (req, res) => {
    sseHub.handleRedeemEvents(req, res);
  });

  app.get('/api/accounts/import-events', requireAuth, (req, res) => {
    sseHub.handleImportEvents(req, res);
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

      sseHub.broadcastImportProgress({
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
        sseHub.broadcastImportProgress({
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
      autoRedeemCoordinator.enqueueLatestRedeemForNewAccounts(result.insertedAccountIds);
      sseHub.broadcastImportProgress({
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
        failed,
        latestRedeemQueued: result.insertedAccountIds.length
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

  app.post('/api/config/redeem-token/fetch', requireRole('admin'), async (_req, res) => {
    try {
      const { token, sourceUrl } = await fetchRemoteRedeemToken();
      setRedeemToken(token);
      res.json({
        ...getRedeemConfig(),
        sourceUrl
      });
    } catch (error) {
      sendJsonError(res, error);
    }
  });

  app.get('/api/redeem-codes', requireAuth, async (req, res) => {
    try {
      const rawLimit = Array.isArray(req.query.limit) ? req.query.limit[0] : req.query.limit;
      const parsedLimit = Number(rawLimit);
      const limit = Number.isFinite(parsedLimit) ? Math.max(1, Math.min(parsedLimit, 200)) : 50;
      const rows = await listRedeemCodes(limit);
      res.json(rows);
    } catch (error) {
      sendJsonError(res, error);
    }
  });

  app.post('/api/redeem-codes/sync', requireRole('admin'), async (_req, res) => {
    try {
      const result = await pollActiveRedeemCodeSource();
      await autoRedeemCoordinator.enqueueAutoRedeemCodes(result.insertedCodes);
      res.json({
        ok: true,
        ...result
      });
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
        retentionDays: visitorLogRetentionDays,
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
}
