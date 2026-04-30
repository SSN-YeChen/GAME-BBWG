import { listAccountsByStatus } from '../core/accountRepository.js';
import { ACCOUNT_STATUS } from '../core/dbTypes.js';
import type { RedeemService } from './redeem.js';
import type { RedeemSummary } from './redeemTypes.js';
import { mergeRedeemSummaries } from './redeemSummary.js';

export async function runAllAccountsRedeemWithSingleFailureRetry(
  redeemService: RedeemService,
  code: string,
  formatLogTime: () => string
): Promise<RedeemSummary> {
  const firstSummary = await redeemService.runAutoRedeemForAllAccounts(code);
  if (firstSummary.failureCount === 0) {
    return firstSummary;
  }

  const failedAccounts = await listAccountsByStatus(ACCOUNT_STATUS.failed);
  const failedAccountIds = failedAccounts.map((account) => account.accountId);
  if (failedAccountIds.length === 0) {
    return firstSummary;
  }

  // eslint-disable-next-line no-console
  console.log(`[${formatLogTime()}] 自动兑换失败账号重试开始：${code}，失败账号数=${failedAccountIds.length}`);
  const retrySummary = await redeemService.runBatchRedeem(code, failedAccountIds);
  // eslint-disable-next-line no-console
  console.log(`[${formatLogTime()}] 自动兑换失败账号重试结束：${code}`);

  return mergeRedeemSummaries(firstSummary, retrySummary);
}

export async function runTargetAccountsRedeemWithSingleFailureRetry(
  redeemService: RedeemService,
  code: string,
  accountIds: string[]
): Promise<RedeemSummary> {
  const firstSummary = await redeemService.runRedeemForAccounts(code, accountIds);
  if (firstSummary.failureCount === 0) {
    return firstSummary;
  }

  const failedAccountIdSet = new Set(accountIds);
  const failedAccountIds = (await listAccountsByStatus(ACCOUNT_STATUS.failed))
    .map((account) => account.accountId)
    .filter((accountId) => failedAccountIdSet.has(accountId));
  if (failedAccountIds.length === 0) {
    return firstSummary;
  }

  // eslint-disable-next-line no-console
  console.log(`new account latest-code redeem retry started: code=${code}, accounts=${failedAccountIds.length}`);
  const retrySummary = await redeemService.runBatchRedeem(code, failedAccountIds);
  // eslint-disable-next-line no-console
  console.log(`new account latest-code redeem retry completed: code=${code}`);

  return mergeRedeemSummaries(firstSummary, retrySummary);
}
