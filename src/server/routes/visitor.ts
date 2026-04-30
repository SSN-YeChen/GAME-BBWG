import {
  deleteAllVisitorLogs,
  deleteBlacklistEntry,
  listBlacklistEntries,
  listVisitorLogs,
  upsertBlacklistEntry
} from '../../core/visitorRepository.js';
import { normalizeIpAddress, sendJsonError } from '../http.js';
import type { ApiRouteContext } from './types.js';

export function registerVisitorRoutes({ app, authService, visitorLogRetentionDays }: ApiRouteContext): void {
  const requireRole = authService.requireRole.bind(authService);

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
