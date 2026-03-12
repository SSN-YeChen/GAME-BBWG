import crypto from 'node:crypto';
import { getRedeemToken } from './config.js';

const LOGIN_URL = 'https://giftcode-api.benbenwangguo.cn/api/player';
const REQUEST_DELAY_MS = 1200;
const RETRY_AFTER_429_MS = 5000;

interface ApiEnvelope {
  code?: number;
  msg?: string;
  data?: Record<string, unknown>;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
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

async function postFormJson(url: string, params: Record<string, string>, timeoutMs: number): Promise<Response> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);

    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams(params),
        signal: controller.signal
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
    }
  }

  throw lastError instanceof Error ? lastError : new Error('Request failed');
}

export async function fetchPlayerProfile(accountId: string): Promise<{ name: string; details: Record<string, unknown> }> {
  const response = await postFormJson(
    LOGIN_URL,
    buildSignedParams({
      fid: accountId
    }),
    10_000
  );

  if (!response.ok) {
    throw new Error(`登录请求失败: HTTP ${response.status}`);
  }

  const result = (await response.json()) as ApiEnvelope;
  if (result.code !== 0 || !result.data) {
    throw new Error(result.msg ?? '接口返回异常');
  }

  const details = result.data;
  const name = typeof details.nickname === 'string' ? details.nickname : '';
  return {
    name,
    details
  };
}

export async function waitForNextAccount(): Promise<void> {
  await sleep(REQUEST_DELAY_MS);
}
