import {
  deleteAccount,
  deleteAllAccounts,
  listAccounts,
  listBlacklistedAccounts,
  reorderAccounts,
  setAccountBlacklist
} from '../../core/db.js';
import { sendJsonError } from '../http.js';
import type { ApiRouteContext } from './types.js';

export function registerAccountRoutes({
  app,
  authService,
  accountImportService,
  autoRedeemCoordinator,
  sseHub
}: ApiRouteContext): void {
  const requireAuth = authService.requireAuth;
  const requireRole = authService.requireRole.bind(authService);

  app.get('/api/accounts/import-events', requireAuth, (req, res) => {
    sseHub.handleImportEvents(req, res);
  });

  app.post('/api/accounts/batch', requireAuth, async (req, res) => {
    try {
      const { accountIds } = req.body as { accountIds?: string[] };
      const result = await accountImportService.importAccounts(Array.isArray(accountIds) ? accountIds : [], (payload) => {
        sseHub.broadcastImportProgress(payload);
      });

      autoRedeemCoordinator.enqueueLatestRedeemForNewAccounts(result.insertedAccountIds);
      res.json({
        inserted: result.inserted,
        skipped: result.skipped,
        failed: result.failed,
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
}
