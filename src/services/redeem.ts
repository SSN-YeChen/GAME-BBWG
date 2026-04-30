import { EventEmitter } from 'node:events';
import { forceSetAllAccountsRedeemed } from '../core/accountRepository.js';
import { RedeemAccountProcessor } from './redeemAccountProcessor.js';
import { countRemainingRedeemAccounts, selectRedeemAccounts } from './redeemAccountSelector.js';
import { RedeemCancelledError } from './redeemCancellation.js';
import { RedeemRunState } from './redeemRunState.js';
import type { RedeemProgressPayload, RedeemRunOptions, RedeemSummary } from './redeemTypes.js';
export type { ApiEnvelope, RedeemProgressPayload, RedeemRunOptions, RedeemSummary } from './redeemTypes.js';

const REQUEST_DELAY_MS = 1200;
const CHUNK_DELAY_MS = 4000;
const CHUNK_SIZE = 30;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

export class RedeemService extends EventEmitter {
  private running = false;
  private cancelRequested = false;
  private activeController: AbortController | null = null;

  private emitProgress(payload: RedeemProgressPayload): void {
    this.emit('progress', payload);
  }

  private log(level: RedeemProgressPayload['level'], message: string): void {
    this.emitProgress({
      type: 'log',
      level,
      message
    });
  }

  isRunning(): boolean {
    return this.running;
  }

  requestCancel(): boolean {
    if (!this.running) {
      return false;
    }

    this.cancelRequested = true;
    this.activeController?.abort();
    this.log('warn', '已收到停止请求，正在终止当前兑换任务...');
    return true;
  }

  private ensureNotCancelled(): void {
    if (this.cancelRequested) {
      throw new RedeemCancelledError();
    }
  }

  private async sleepWithCancel(ms: number): Promise<void> {
    const slice = 100;
    let remaining = ms;

    while (remaining > 0) {
      this.ensureNotCancelled();
      const waitMs = Math.min(slice, remaining);
      await sleep(waitMs);
      remaining -= waitMs;
    }
  }

  async runBatchRedeem(
    giftCode: string,
    targetAccountIds?: string[],
    options?: RedeemRunOptions
  ): Promise<RedeemSummary> {
    if (this.running) {
      throw new Error('当前已有兑换任务正在执行，请稍后再试。');
    }

    this.running = true;
    this.cancelRequested = false;
    let runState = new RedeemRunState(0, false);
    try {
      const trimmedCode = giftCode.trim();
      if (!trimmedCode) {
        throw new Error('请输入兑换码');
      }

      const includeAllAccounts = options?.includeAllAccounts ?? false;
      const includeTargetAccounts = options?.includeTargetAccounts ?? false;
      const selected = await selectRedeemAccounts(targetAccountIds, options);
      const pendingAccounts = selected.accounts;
      runState = new RedeemRunState(pendingAccounts.length, selected.resetTriggered);

      if (runState.total === 0) {
        throw new Error(targetAccountIds && targetAccountIds.length > 0 ? '没有可重试的失败账号' : '没有可用账号');
      }

      this.emitProgress({
        type: 'start',
        total: runState.total,
        processed: 0
      });

      if (runState.resetTriggered) {
        this.log('warn', '开始兑换前，已将 status=1 的账号重置为 0。');
      }

      this.log('info', `共找到 ${runState.total} 个账号，开始处理...`);
      const accountProcessor = new RedeemAccountProcessor({
        ensureNotCancelled: () => this.ensureNotCancelled(),
        sleepWithCancel: (ms) => this.sleepWithCancel(ms),
        setActiveController: (controller) => {
          this.activeController = controller;
        },
        log: (level, message) => this.log(level, message)
      });

      for (let index = 0; index < pendingAccounts.length; index += CHUNK_SIZE) {
        this.ensureNotCancelled();
        const chunk = pendingAccounts.slice(index, index + CHUNK_SIZE);

        for (const account of chunk) {
          this.ensureNotCancelled();
          try {
            const result = await accountProcessor.processAccount(account, trimmedCode);
            runState.applyAccountResult(result);
          } finally {
            this.activeController = null;
            runState.markProcessed();
            this.emitProgress({
              type: 'progress',
              processed: runState.processed,
              total: runState.total
            });
            if (!this.cancelRequested) {
              await this.sleepWithCancel(REQUEST_DELAY_MS);
            }
          }
        }

        if (index + CHUNK_SIZE < pendingAccounts.length) {
          this.log('info', '休息 4 秒后继续...');
          await this.sleepWithCancel(CHUNK_DELAY_MS);
        }
      }

      const remaining = includeAllAccounts || includeTargetAccounts ? 0 : await countRemainingRedeemAccounts(options);
      const summary = runState.toSummary(remaining);

      if (remaining > 0) {
        this.log('warn', `还有 ${remaining} 个账号未处理，建议重新运行一次。`);
      }

      this.log('info', '兑换流程执行完毕。');
      this.emitProgress({
        type: 'done',
        summary
      });

      return summary;
    } catch (error) {
      if (error instanceof RedeemCancelledError) {
        const remaining = await countRemainingRedeemAccounts(options);
        const summary = runState.toSummary(remaining);

        this.log('warn', '兑换任务已停止。');
        this.emitProgress({
          type: 'done',
          summary
        });

        throw error;
      }

      throw error;
    } finally {
      this.running = false;
      this.cancelRequested = false;
      this.activeController = null;
    }
  }

  async forceCompleteAllRedeem(): Promise<{ updated: number }> {
    return { updated: await forceSetAllAccountsRedeemed() };
  }

  async runAutoRedeemForAllAccounts(giftCode: string): Promise<RedeemSummary> {
    return this.runBatchRedeem(giftCode, undefined, { includeAllAccounts: true });
  }

  async runRedeemForAccounts(giftCode: string, accountIds: string[]): Promise<RedeemSummary> {
    return this.runBatchRedeem(giftCode, accountIds, { includeTargetAccounts: true });
  }
}
