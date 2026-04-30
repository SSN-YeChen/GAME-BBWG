import { listRedeemCodes } from '../../core/redeemCodeRepository.js';
import { sendJsonError } from '../http.js';
import type { ApiRouteContext } from './types.js';

export function registerRedeemRoutes({
  app,
  authService,
  redeemService,
  autoRedeemCoordinator,
  sseHub,
  pollActiveRedeemCodeSource
}: ApiRouteContext): void {
  const requireAuth = authService.requireAuth;
  const requireRole = authService.requireRole.bind(authService);

  app.get('/api/redeem/events', requireAuth, (req, res) => {
    sseHub.handleRedeemEvents(req, res);
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
}
