import { EventEmitter } from 'node:events';
import {
  ACCOUNT_STATUS,
  forceSetAllAccountsRedeemed,
  listAccountsByIdsIncludingDeleted,
  updateAccountProfile,
  updateAccountStatus
} from '../core/db.js';
import { countRemainingRedeemAccounts, selectRedeemAccounts } from './redeemAccountSelector.js';
import { isTimeoutRetryMessage, submitLoginRequest, submitRedeemRequest } from './redeemClient.js';
import type { ApiEnvelope, RedeemProgressPayload, RedeemRunOptions, RedeemSummary } from './redeemTypes.js';
export type { ApiEnvelope, RedeemProgressPayload, RedeemRunOptions, RedeemSummary } from './redeemTypes.js';

const REQUEST_DELAY_MS = 1200;
const LOGIN_TO_REDEEM_DELAY_MS = 200;
const CHUNK_DELAY_MS = 4000;
const CHUNK_SIZE = 30;
const TIMEOUT_RETRY_DELAY_MS = 2000;
const MAX_TIMEOUT_RETRY_ATTEMPTS = 2;

class RedeemCancelledError extends Error {
  constructor() {
    super('兑换已手动停止。');
    this.name = 'RedeemCancelledError';
  }
}

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

  private async submitRedeemWithTimeoutRetry(accountId: string, giftCode: string): Promise<ApiEnvelope> {
    let lastResult: ApiEnvelope = { msg: '未知错误' };

    for (let attempt = 0; attempt < MAX_TIMEOUT_RETRY_ATTEMPTS; attempt += 1) {
      this.ensureNotCancelled();
      this.activeController = new AbortController();

      lastResult = await submitRedeemRequest(accountId, giftCode, this.activeController);
      const message = lastResult.msg ?? '未知错误';
      if (!isTimeoutRetryMessage(message) || attempt === MAX_TIMEOUT_RETRY_ATTEMPTS - 1) {
        return lastResult;
      }

      this.log('warn', `兑换返回 TIMEOUT RETRY.，2 秒后重试 (${accountId})`);
      await this.sleepWithCancel(TIMEOUT_RETRY_DELAY_MS);
    }

    return lastResult;
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
    let total = 0;
    let processed = 0;
    let successCount = 0;
    let receivedCount = 0;
    let failureCount = 0;
    let resetTriggered = false;
    try {
      const trimmedCode = giftCode.trim();
      if (!trimmedCode) {
        throw new Error('请输入兑换码');
      }

      const includeAllAccounts = options?.includeAllAccounts ?? false;
      const includeTargetAccounts = options?.includeTargetAccounts ?? false;
      const selected = await selectRedeemAccounts(targetAccountIds, options);
      const pendingAccounts = selected.accounts;
      resetTriggered = selected.resetTriggered;

      total = pendingAccounts.length;
      if (total === 0) {
        throw new Error(targetAccountIds && targetAccountIds.length > 0 ? '没有可重试的失败账号' : '没有可用账号');
      }

      this.emitProgress({
        type: 'start',
        total,
        processed: 0
      });

      if (resetTriggered) {
        this.log('warn', '开始兑换前，已将 status=1 的账号重置为 0。');
      }

      this.log('info', `共找到 ${total} 个账号，开始处理...`);

      for (let index = 0; index < pendingAccounts.length; index += CHUNK_SIZE) {
        this.ensureNotCancelled();
        const chunk = pendingAccounts.slice(index, index + CHUNK_SIZE);

        for (const account of chunk) {
          this.ensureNotCancelled();
          const displayName = account.name?.trim() || '未命名账号';
          try {
            const [latestAccount] = await listAccountsByIdsIncludingDeleted([account.accountId], { includeBlacklisted: true });
            if (!latestAccount || latestAccount.deleted) {
              this.log('warn', `已跳过已删除账号: ${displayName} (${account.accountId})`);
              continue;
            }
            if (latestAccount.blacklisted) {
              this.log('warn', `已跳过黑名单账号: ${displayName} (${account.accountId})`);
              continue;
            }

            this.log('info', `开始处理: ${displayName} (${account.accountId})`);
            this.activeController = new AbortController();

            const loginResponse = await submitLoginRequest(account.accountId, this.activeController);

            if (!loginResponse.ok) {
              failureCount += 1;
              await updateAccountStatus(account.accountId, ACCOUNT_STATUS.failed);
              this.log('error', `登录请求失败: HTTP ${loginResponse.status} (${account.accountId})`);
              continue;
            }

            const loginResult = (await loginResponse.json()) as ApiEnvelope;
            if (loginResult.code !== 0 || !loginResult.data) {
              failureCount += 1;
              await updateAccountStatus(account.accountId, ACCOUNT_STATUS.failed);
              this.log('error', `登录失败: ${loginResult.msg ?? '接口返回异常'} (${account.accountId})`);
              continue;
            }

            const profileData = loginResult.data;
            const nickname = typeof profileData.nickname === 'string' ? profileData.nickname : account.name ?? '';
            await updateAccountProfile(account.accountId, {
              name: nickname,
              details: profileData
            });

            this.log('success', `登录成功: ${nickname || account.accountId} (${account.accountId})`);
            await this.sleepWithCancel(LOGIN_TO_REDEEM_DELAY_MS);
            const redeemResult = await this.submitRedeemWithTimeoutRetry(account.accountId, trimmedCode);
            const code = redeemResult.code ?? null;
            const message = redeemResult.msg ?? '未知错误';

            if (code === 0 || message.toUpperCase() === 'RECEIVED.') {
              await updateAccountStatus(account.accountId, ACCOUNT_STATUS.redeemed);
              if (code === 0) {
                successCount += 1;
                this.log('success', `兑换成功: ${nickname || account.accountId} (${account.accountId})`);
              } else {
                receivedCount += 1;
                this.log('warn', `已领取: ${nickname || account.accountId} (${account.accountId})`);
              }
            } else {
              failureCount += 1;
              await updateAccountStatus(account.accountId, ACCOUNT_STATUS.failed);
              this.log('warn', `兑换失败: ${nickname || account.accountId} (${account.accountId}) - ${message}`);
            }
          } catch (error) {
            if (error instanceof RedeemCancelledError) {
              throw error;
            }

            failureCount += 1;
            const message =
              error instanceof Error && error.name === 'AbortError'
                ? '请求已中止'
                : error instanceof Error && error.message.startsWith('HTTP ')
                  ? error.message
                : error instanceof Error
                  ? error.message
                  : '未知错误';
            await updateAccountStatus(account.accountId, ACCOUNT_STATUS.failed);
            if (error instanceof Error && error.message.startsWith('HTTP ')) {
              this.log('error', `兑换请求失败: ${message} (${account.accountId})`);
            } else {
              this.log('error', `异常: ${message} (${account.accountId})`);
            }
          } finally {
            this.activeController = null;
            processed += 1;
            this.emitProgress({
              type: 'progress',
              processed,
              total
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
      const summary: RedeemSummary = {
        total,
        processed,
        successCount,
        receivedCount,
        failureCount,
        remaining,
        resetTriggered
      };

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
        const summary: RedeemSummary = {
          total,
          processed,
          successCount,
          receivedCount,
          failureCount,
          remaining,
          resetTriggered
        };

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
