import crypto from 'node:crypto';
import { getRedeemToken } from '../core/config.js';

export const GIFT_CODE_API_URLS = {
  login: 'https://giftcode-api.benbenwangguo.cn/api/player',
  redeem: 'https://giftcode-api.benbenwangguo.cn/api/gift_code'
} as const;

const RETRY_AFTER_429_MS = 5000;

export function sleep(ms: number): Promise<void> {
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

export async function postSignedFormJson(
  url: string,
  params: Record<string, string>,
  timeoutMs: number,
  activeController?: AbortController
): Promise<Response> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const timeoutController = new AbortController();
    const onManualAbort = () => timeoutController.abort();
    const timer = setTimeout(() => timeoutController.abort(), timeoutMs);

    activeController?.signal.addEventListener('abort', onManualAbort, { once: true });
    try {
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: new URLSearchParams(buildSignedParams(params)),
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
      activeController?.signal.removeEventListener('abort', onManualAbort);
    }
  }

  throw lastError instanceof Error ? lastError : new Error('Request failed');
}
