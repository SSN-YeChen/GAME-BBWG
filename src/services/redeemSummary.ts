import type { RedeemSummary } from './redeemTypes.js';

export function mergeRedeemSummaries(firstSummary: RedeemSummary, retrySummary: RedeemSummary): RedeemSummary {
  return {
    total: firstSummary.total + retrySummary.total,
    processed: firstSummary.processed + retrySummary.processed,
    successCount: firstSummary.successCount + retrySummary.successCount,
    receivedCount: firstSummary.receivedCount + retrySummary.receivedCount,
    failureCount: retrySummary.failureCount,
    remaining: retrySummary.remaining,
    resetTriggered: firstSummary.resetTriggered || retrySummary.resetTriggered
  };
}
