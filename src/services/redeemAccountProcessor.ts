import {
  listAccountsByIdsIncludingDeleted,
  updateAccountProfile,
  updateAccountStatus
} from '../core/accountRepository.js';
import { ACCOUNT_STATUS, type AccountRow } from '../core/dbTypes.js';
import { RedeemCancelledError } from './redeemCancellation.js';
import { isTimeoutRetryMessage, submitLoginRequest, submitRedeemRequest } from './redeemClient.js';
import type { ApiEnvelope, RedeemProgressPayload } from './redeemTypes.js';

const LOGIN_TO_REDEEM_DELAY_MS = 200;
const TIMEOUT_RETRY_DELAY_MS = 2000;
const MAX_TIMEOUT_RETRY_ATTEMPTS = 2;

export interface RedeemAccountResult {
  successCount: number;
  receivedCount: number;
  failureCount: number;
}

export class RedeemAccountProcessor {
  constructor(
    private readonly options: {
      ensureNotCancelled: () => void;
      sleepWithCancel: (ms: number) => Promise<void>;
      setActiveController: (controller: AbortController | null) => void;
      log: (level: RedeemProgressPayload['level'], message: string) => void;
    }
  ) {}

  async processAccount(account: AccountRow, giftCode: string): Promise<RedeemAccountResult> {
    const displayName = account.name?.trim() || '未命名账号';
    try {
      const [latestAccount] = await listAccountsByIdsIncludingDeleted([account.accountId], { includeBlacklisted: true });
      if (!latestAccount || latestAccount.deleted) {
        this.options.log('warn', `已跳过已删除账号: ${displayName} (${account.accountId})`);
        return this.emptyResult();
      }
      if (latestAccount.blacklisted) {
        this.options.log('warn', `已跳过黑名单账号: ${displayName} (${account.accountId})`);
        return this.emptyResult();
      }

      this.options.log('info', `开始处理: ${displayName} (${account.accountId})`);
      const loginController = new AbortController();
      this.options.setActiveController(loginController);
      const loginResponse = await submitLoginRequest(account.accountId, loginController);

      if (!loginResponse.ok) {
        await updateAccountStatus(account.accountId, ACCOUNT_STATUS.failed);
        this.options.log('error', `登录请求失败: HTTP ${loginResponse.status} (${account.accountId})`);
        return { successCount: 0, receivedCount: 0, failureCount: 1 };
      }

      const loginResult = (await loginResponse.json()) as ApiEnvelope;
      if (loginResult.code !== 0 || !loginResult.data) {
        await updateAccountStatus(account.accountId, ACCOUNT_STATUS.failed);
        this.options.log('error', `登录失败: ${loginResult.msg ?? '接口返回异常'} (${account.accountId})`);
        return { successCount: 0, receivedCount: 0, failureCount: 1 };
      }

      const profileData = loginResult.data;
      const nickname = typeof profileData.nickname === 'string' ? profileData.nickname : account.name ?? '';
      await updateAccountProfile(account.accountId, {
        name: nickname,
        details: profileData
      });

      this.options.log('success', `登录成功: ${nickname || account.accountId} (${account.accountId})`);
      await this.options.sleepWithCancel(LOGIN_TO_REDEEM_DELAY_MS);
      const redeemResult = await this.submitRedeemWithTimeoutRetry(account.accountId, giftCode);
      const code = redeemResult.code ?? null;
      const message = redeemResult.msg ?? '未知错误';

      if (code === 0 || message.toUpperCase() === 'RECEIVED.') {
        await updateAccountStatus(account.accountId, ACCOUNT_STATUS.redeemed);
        if (code === 0) {
          this.options.log('success', `兑换成功: ${nickname || account.accountId} (${account.accountId})`);
          return { successCount: 1, receivedCount: 0, failureCount: 0 };
        }

        this.options.log('warn', `已领取: ${nickname || account.accountId} (${account.accountId})`);
        return { successCount: 0, receivedCount: 1, failureCount: 0 };
      }

      await updateAccountStatus(account.accountId, ACCOUNT_STATUS.failed);
      this.options.log('warn', `兑换失败: ${nickname || account.accountId} (${account.accountId}) - ${message}`);
      return { successCount: 0, receivedCount: 0, failureCount: 1 };
    } catch (error) {
      if (error instanceof RedeemCancelledError) {
        throw error;
      }

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
        this.options.log('error', `兑换请求失败: ${message} (${account.accountId})`);
      } else {
        this.options.log('error', `异常: ${message} (${account.accountId})`);
      }
      return { successCount: 0, receivedCount: 0, failureCount: 1 };
    }
  }

  private async submitRedeemWithTimeoutRetry(accountId: string, giftCode: string): Promise<ApiEnvelope> {
    let lastResult: ApiEnvelope = { msg: '未知错误' };

    for (let attempt = 0; attempt < MAX_TIMEOUT_RETRY_ATTEMPTS; attempt += 1) {
      this.options.ensureNotCancelled();
      const redeemController = new AbortController();
      this.options.setActiveController(redeemController);

      lastResult = await submitRedeemRequest(accountId, giftCode, redeemController);
      const message = lastResult.msg ?? '未知错误';
      if (!isTimeoutRetryMessage(message) || attempt === MAX_TIMEOUT_RETRY_ATTEMPTS - 1) {
        return lastResult;
      }

      this.options.log('warn', `兑换返回 TIMEOUT RETRY.，2 秒后重试 (${accountId})`);
      await this.options.sleepWithCancel(TIMEOUT_RETRY_DELAY_MS);
    }

    return lastResult;
  }

  private emptyResult(): RedeemAccountResult {
    return { successCount: 0, receivedCount: 0, failureCount: 0 };
  }
}
