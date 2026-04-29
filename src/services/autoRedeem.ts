import {
  ACCOUNT_STATUS,
  completeRedeemCodeRedemption,
  failRedeemCodeRedemption,
  listAccountsByStatus,
  listRedeemCodes,
  reserveRedeemCodeRedemption
} from '../core/db.js';
import { getRedeemToken, setRedeemToken } from '../core/config.js';
import { RedeemService, type RedeemSummary } from './redeem.js';
import { fetchRemoteRedeemToken } from './redeemToken.js';

const AUTO_REDEEM_MAX_CODE_AGE_MS = 1000 * 60 * 60 * 24;

function formatLogTime(): string {
  return new Date().toLocaleString('zh-CN', { hour12: false });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export class AutoRedeemCoordinator {
  private readonly autoRedeemQueue: string[] = [];
  private readonly autoRedeemQueuedCodes = new Set<string>();
  private autoRedeemQueueRunning = false;
  private readonly newAccountRedeemQueue: string[] = [];
  private readonly newAccountRedeemQueuedIds = new Set<string>();
  private newAccountRedeemQueueRunning = false;
  private redeemTaskChain: Promise<void> = Promise.resolve();

  constructor(
    private readonly options: {
      redeemService: RedeemService;
      pauseSourcePolling: () => void;
      resumeSourcePolling: () => void;
    }
  ) {}

  async enqueueAutoRedeemCodes(codes: string[]): Promise<void> {
    const normalizedCodes = Array.from(new Set(codes.map((code) => code.trim().toUpperCase()).filter(Boolean)));
    if (normalizedCodes.length === 0) {
      return;
    }

    const redeemCodes = await listRedeemCodes(200);
    const redeemCodeMap = new Map(redeemCodes.map((item) => [item.code, item]));
    const now = Date.now();

    for (const normalizedCode of normalizedCodes) {
      if (!normalizedCode || this.autoRedeemQueuedCodes.has(normalizedCode)) {
        continue;
      }

      const redeemCode = redeemCodeMap.get(normalizedCode);
      const publishedAt = redeemCode?.publishedAt ?? 0;
      if (publishedAt <= 0 || now - publishedAt > AUTO_REDEEM_MAX_CODE_AGE_MS) {
        // eslint-disable-next-line no-console
        console.log(`auto redeem skipped for old code ${normalizedCode}, publishedAt=${publishedAt || 'unknown'}`);
        continue;
      }

      this.autoRedeemQueuedCodes.add(normalizedCode);
      this.autoRedeemQueue.push(normalizedCode);
    }

    void this.drainAutoRedeemQueue();
  }

  enqueueLatestRedeemForNewAccounts(accountIds: string[]): void {
    for (const accountId of accountIds) {
      const normalizedAccountId = accountId.trim();
      if (!normalizedAccountId || this.newAccountRedeemQueuedIds.has(normalizedAccountId)) {
        continue;
      }

      this.newAccountRedeemQueuedIds.add(normalizedAccountId);
      this.newAccountRedeemQueue.push(normalizedAccountId);
    }

    void this.drainNewAccountRedeemQueue();
  }

  private async runRedeemTaskExclusive<T>(task: () => Promise<T>): Promise<T> {
    const previousTask = this.redeemTaskChain;
    let releaseTask: () => void = () => undefined;
    this.redeemTaskChain = new Promise<void>((resolve) => {
      releaseTask = resolve;
    });

    await previousTask;
    try {
      while (this.options.redeemService.isRunning()) {
        await sleep(5000);
      }
      return await task();
    } finally {
      releaseTask();
    }
  }

  private async drainAutoRedeemQueue(): Promise<void> {
    if (this.autoRedeemQueueRunning) {
      return;
    }

    this.autoRedeemQueueRunning = true;
    this.options.pauseSourcePolling();
    try {
      while (this.autoRedeemQueue.length > 0) {
        const code = this.autoRedeemQueue.shift();
        if (!code) {
          continue;
        }

        try {
          const reserved = await reserveRedeemCodeRedemption(code);
          if (!reserved) {
            continue;
          }

          const summary = await this.runRedeemTaskExclusive(async () => {
            await this.ensureRedeemTokenForAutoRedeem();
            // eslint-disable-next-line no-console
            console.log(`[${formatLogTime()}] 自动兑换开始：${code}`);
            return this.runAutoRedeemWithSingleFailureRetry(code);
          });
          await completeRedeemCodeRedemption(code, summary);
          // eslint-disable-next-line no-console
          console.log(`[${formatLogTime()}] 自动兑换结束：${code}`);
        } catch (error) {
          const message = error instanceof Error ? error.message : '未知错误';
          await failRedeemCodeRedemption(code, message).catch((persistError: unknown) => {
            // eslint-disable-next-line no-console
            console.error('failed to persist auto redeem failure', persistError);
          });
          // eslint-disable-next-line no-console
          console.error(`[${formatLogTime()}] 自动兑换失败：${code}`, error);
        } finally {
          this.autoRedeemQueuedCodes.delete(code);
        }
      }
    } finally {
      this.options.resumeSourcePolling();
      this.autoRedeemQueueRunning = false;
    }
  }

  private async ensureRedeemTokenForAutoRedeem(): Promise<void> {
    if (getRedeemToken()) {
      return;
    }

    // eslint-disable-next-line no-console
    console.log('redeem token is empty, fetching before auto redeem...');
    const { token, sourceUrl } = await fetchRemoteRedeemToken();
    setRedeemToken(token);
    // eslint-disable-next-line no-console
    console.log(`redeem token fetched before auto redeem: ${sourceUrl}`);
  }

  private async runAutoRedeemWithSingleFailureRetry(code: string): Promise<RedeemSummary> {
    const firstSummary = await this.options.redeemService.runAutoRedeemForAllAccounts(code);
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
    const retrySummary = await this.options.redeemService.runBatchRedeem(code, failedAccountIds);
    // eslint-disable-next-line no-console
    console.log(`[${formatLogTime()}] 自动兑换失败账号重试结束：${code}`);

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

  private async drainNewAccountRedeemQueue(): Promise<void> {
    if (this.newAccountRedeemQueueRunning) {
      return;
    }

    this.newAccountRedeemQueueRunning = true;
    try {
      while (this.newAccountRedeemQueue.length > 0) {
        const accountIds = this.newAccountRedeemQueue.splice(0, this.newAccountRedeemQueue.length);
        try {
          const [latestCode] = await listRedeemCodes(1);
          if (!latestCode) {
            // eslint-disable-next-line no-console
            console.log(`new account latest-code redeem skipped, no redeem code found. accounts=${accountIds.length}`);
            continue;
          }

          await this.runRedeemTaskExclusive(async () => {
            await this.ensureRedeemTokenForAutoRedeem();
            // eslint-disable-next-line no-console
            console.log(`new account latest-code redeem started: code=${latestCode.code}, accounts=${accountIds.length}`);
            const summary = await this.runNewAccountRedeemWithSingleFailureRetry(latestCode.code, accountIds);
            // eslint-disable-next-line no-console
            console.log(
              `new account latest-code redeem completed: code=${latestCode.code}, processed=${summary.processed}, failed=${summary.failureCount}`
            );
          });
        } catch (error) {
          // eslint-disable-next-line no-console
          console.error('new account latest-code redeem failed', error);
        } finally {
          for (const accountId of accountIds) {
            this.newAccountRedeemQueuedIds.delete(accountId);
          }
        }
      }
    } finally {
      this.newAccountRedeemQueueRunning = false;
    }
  }

  private async runNewAccountRedeemWithSingleFailureRetry(code: string, accountIds: string[]): Promise<RedeemSummary> {
    const firstSummary = await this.options.redeemService.runRedeemForAccounts(code, accountIds);
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
    const retrySummary = await this.options.redeemService.runBatchRedeem(code, failedAccountIds);
    // eslint-disable-next-line no-console
    console.log(`new account latest-code redeem retry completed: code=${code}`);

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
}
