import crypto from 'node:crypto';
import { EventEmitter } from 'node:events';
import {
  ACCOUNT_STATUS,
  countAccountsByStatus,
  forceSetAllAccountsRedeemed,
  listAccounts,
  listAccountsByIds,
  listAccountsByIdsIncludingDeleted,
  listAccountsByStatus,
  resetAccountsStatus,
  updateAccountProfile,
  updateAccountStatus
} from './db.js';
import { getRedeemToken } from './config.js';

const LOGIN_URL = 'https://giftcode-api.benbenwangguo.cn/api/player';
const REDEEM_URL = 'https://giftcode-api.benbenwangguo.cn/api/gift_code';
const REQUEST_DELAY_MS = 1200;
const LOGIN_TO_REDEEM_DELAY_MS = 200;
const CHUNK_DELAY_MS = 4000;
const CHUNK_SIZE = 30;
const RETRY_AFTER_429_MS = 5000;
const TIMEOUT_RETRY_DELAY_MS = 2000;
const MAX_TIMEOUT_RETRY_ATTEMPTS = 2;

export interface RedeemSummary {
  total: number;
  processed: number;
  successCount: number;
  receivedCount: number;
  failureCount: number;
  remaining: number;
  resetTriggered: boolean;
}

export interface RedeemProgressPayload {
  type: 'start' | 'log' | 'progress' | 'done';
  level?: 'info' | 'warn' | 'error' | 'success';
  message?: string;
  processed?: number;
  total?: number;
  summary?: RedeemSummary;
}

export interface ApiEnvelope {
  code?: number;
  msg?: string;
  data?: Record<string, unknown>;
}

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

function isTimeoutRetryMessage(message: string): boolean {
  return message.trim().toUpperCase() === 'TIMEOUT RETRY.';
}

async function postFormJson(
  url: string,
  params: Record<string, string>,
  timeoutMs: number,
  activeController: AbortController
): Promise<Response> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const timeoutController = new AbortController();
    const onManualAbort = () => timeoutController.abort();
    const timer = setTimeout(() => timeoutController.abort(), timeoutMs);

    activeController.signal.addEventListener('abort', onManualAbort, { once: true });
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams(params),
        signal: timeoutController.signal
      });

      if (response.status === 429 && attempt < 2) {
        await sleep(RETRY_AFTER_429_MS);
        continue;
      }

      return response;
    } catch (error) {
      lastError = error;
      if (attempt < 2) {
        await sleep(1000);
      }
    } finally {
      clearTimeout(timer);
      activeController.signal.removeEventListener('abort', onManualAbort);
    }
  }

  throw lastError instanceof Error ? lastError : new Error('Request failed');
}

function normalizeSignValue(value: unknown): string {
  if (Array.isArray(value) || (typeof value === 'object' && value !== null)) {
    return JSON.stringify(value);
  }
  return String(value);
}

function buildSignedParams(params: Record<string, string>): Record<string, string> {
  const redeemToken = getRedeemToken();
  if (!redeemToken) {
    throw new Error('缺少兑换 TOKEN，请先在批量兑换页面保存 TOKEN。');
  }

  const signedParams: Record<string, string> = {
    ...params,
    time: Date.now().toString()
  };

  const sortedEntries = Object.entries(signedParams).sort(([a], [b]) => a.localeCompare(b, 'en'));
  const payload = sortedEntries.map(([k, v]) => `${k}=${normalizeSignValue(v)}`).join('&');
  signedParams.sign = crypto.createHash('md5').update(`${payload}${redeemToken}`).digest('hex');
  return signedParams;
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

      const redeemResponse = await postFormJson(
        REDEEM_URL,
        buildSignedParams({
          fid: accountId,
          cdk: giftCode,
          captcha_code: ''
        }),
        15_000,
        this.activeController
      );

      if (!redeemResponse.ok) {
        throw new Error(`HTTP ${redeemResponse.status}`);
      }

      lastResult = (await redeemResponse.json()) as ApiEnvelope;
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
    options?: { includeAllAccounts?: boolean; includeTargetAccounts?: boolean }
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
      let pendingAccounts = includeAllAccounts
        ? await listAccounts()
        : targetAccountIds && targetAccountIds.length > 0
          ? includeTargetAccounts
            ? await listAccountsByIds(targetAccountIds)
            : (await listAccountsByIds(targetAccountIds)).filter((item) => item.status === ACCOUNT_STATUS.failed)
          : await listAccountsByStatus(ACCOUNT_STATUS.pending);

      if (!includeAllAccounts && (!targetAccountIds || targetAccountIds.length === 0) && pendingAccounts.length === 0) {
        await resetAccountsStatus(ACCOUNT_STATUS.redeemed, ACCOUNT_STATUS.pending);
        pendingAccounts = await listAccountsByStatus(ACCOUNT_STATUS.pending);
        resetTriggered = true;
      }

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

            const loginResponse = await postFormJson(
              LOGIN_URL,
              buildSignedParams({
                fid: account.accountId
              }),
              10_000,
              this.activeController
            );

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

      const remaining = includeAllAccounts || includeTargetAccounts ? 0 : await countAccountsByStatus(ACCOUNT_STATUS.pending);
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
        const remaining =
          options?.includeAllAccounts || options?.includeTargetAccounts ? 0 : await countAccountsByStatus(ACCOUNT_STATUS.pending);
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
