import {
  completeRedeemCodeRedemption,
  failRedeemCodeRedemption,
  listRedeemCodes,
  reserveRedeemCodeRedemption
} from '../core/redeemCodeRepository.js';
import { RedeemService } from './redeem.js';
import {
  runAllAccountsRedeemWithSingleFailureRetry,
  runTargetAccountsRedeemWithSingleFailureRetry
} from './autoRedeemRetry.js';
import { ensureRedeemTokenForAutoRedeem } from './autoRedeemToken.js';
import { ExclusiveTaskRunner } from './exclusiveTaskRunner.js';
import { UniqueStringQueue } from './uniqueStringQueue.js';

const AUTO_REDEEM_MAX_CODE_AGE_MS = 1000 * 60 * 60 * 24;

function formatLogTime(): string {
  return new Date().toLocaleString('zh-CN', { hour12: false });
}

export class AutoRedeemCoordinator {
  private readonly autoRedeemQueue = new UniqueStringQueue();
  private autoRedeemQueueRunning = false;
  private readonly newAccountRedeemQueue = new UniqueStringQueue();
  private newAccountRedeemQueueRunning = false;
  private readonly redeemTaskRunner: ExclusiveTaskRunner;

  constructor(
    private readonly options: {
      redeemService: RedeemService;
      pauseSourcePolling: () => void;
      resumeSourcePolling: () => void;
    }
  ) {
    this.redeemTaskRunner = new ExclusiveTaskRunner({
      isBlocked: () => this.options.redeemService.isRunning()
    });
  }

  async enqueueAutoRedeemCodes(codes: string[]): Promise<void> {
    const normalizedCodes = Array.from(new Set(codes.map((code) => code.trim().toUpperCase()).filter(Boolean)));
    if (normalizedCodes.length === 0) {
      return;
    }

    const redeemCodes = await listRedeemCodes(200);
    const redeemCodeMap = new Map(redeemCodes.map((item) => [item.code, item]));
    const now = Date.now();

    for (const normalizedCode of normalizedCodes) {
      const redeemCode = redeemCodeMap.get(normalizedCode);
      const publishedAt = redeemCode?.publishedAt ?? 0;
      if (publishedAt <= 0 || now - publishedAt > AUTO_REDEEM_MAX_CODE_AGE_MS) {
        // eslint-disable-next-line no-console
        console.log(`auto redeem skipped for old code ${normalizedCode}, publishedAt=${publishedAt || 'unknown'}`);
        continue;
      }

      this.autoRedeemQueue.enqueue(normalizedCode);
    }

    void this.drainAutoRedeemQueue();
  }

  enqueueLatestRedeemForNewAccounts(accountIds: string[]): void {
    for (const accountId of accountIds) {
      this.newAccountRedeemQueue.enqueue(accountId);
    }

    void this.drainNewAccountRedeemQueue();
  }

  private async drainAutoRedeemQueue(): Promise<void> {
    if (this.autoRedeemQueueRunning) {
      return;
    }

    this.autoRedeemQueueRunning = true;
    this.options.pauseSourcePolling();
    try {
      while (this.autoRedeemQueue.length > 0) {
        const code = this.autoRedeemQueue.dequeue();
        if (!code) {
          continue;
        }

        try {
          const reserved = await reserveRedeemCodeRedemption(code);
          if (!reserved) {
            continue;
          }

          const summary = await this.redeemTaskRunner.run(async () => {
            await ensureRedeemTokenForAutoRedeem();
            // eslint-disable-next-line no-console
            console.log(`[${formatLogTime()}] 自动兑换开始：${code}`);
            return runAllAccountsRedeemWithSingleFailureRetry(this.options.redeemService, code, formatLogTime);
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
          this.autoRedeemQueue.release(code);
        }
      }
    } finally {
      this.options.resumeSourcePolling();
      this.autoRedeemQueueRunning = false;
    }
  }

  private async drainNewAccountRedeemQueue(): Promise<void> {
    if (this.newAccountRedeemQueueRunning) {
      return;
    }

    this.newAccountRedeemQueueRunning = true;
    try {
      while (this.newAccountRedeemQueue.length > 0) {
        const accountIds = this.newAccountRedeemQueue.drainAll();
        try {
          const [latestCode] = await listRedeemCodes(1);
          if (!latestCode) {
            // eslint-disable-next-line no-console
            console.log(`new account latest-code redeem skipped, no redeem code found. accounts=${accountIds.length}`);
            continue;
          }

          await this.redeemTaskRunner.run(async () => {
            await ensureRedeemTokenForAutoRedeem();
            // eslint-disable-next-line no-console
            console.log(`new account latest-code redeem started: code=${latestCode.code}, accounts=${accountIds.length}`);
            const summary = await runTargetAccountsRedeemWithSingleFailureRetry(
              this.options.redeemService,
              latestCode.code,
              accountIds
            );
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
            this.newAccountRedeemQueue.release(accountId);
          }
        }
      }
    } finally {
      this.newAccountRedeemQueueRunning = false;
    }
  }
}
