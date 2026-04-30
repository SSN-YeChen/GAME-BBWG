import { cleanupVisitorLogs } from '../core/visitorRepository.js';

export const VISITOR_LOG_RETENTION_DAYS = 30;

const VISITOR_LOG_CLEANUP_INTERVAL_MS = 1000 * 60 * 60 * 24;

export function startVisitorLogCleanup(retentionDays = VISITOR_LOG_RETENTION_DAYS): void {
  void cleanupVisitorLogs(retentionDays).catch((error: unknown) => {
    // eslint-disable-next-line no-console
    console.error('failed to cleanup visitor logs on startup', error);
  });

  setInterval(() => {
    void cleanupVisitorLogs(retentionDays).catch((error: unknown) => {
      // eslint-disable-next-line no-console
      console.error('failed to cleanup visitor logs', error);
    });
  }, VISITOR_LOG_CLEANUP_INTERVAL_MS);
}
