import { GIFT_CODE_API_URLS, postSignedFormJson, sleep } from './giftCodeApi.js';

const REQUEST_DELAY_MS = 1200;

interface ApiEnvelope {
  code?: number;
  msg?: string;
  data?: Record<string, unknown>;
}

export async function fetchPlayerProfile(accountId: string): Promise<{ name: string; details: Record<string, unknown> }> {
  const response = await postSignedFormJson(
    GIFT_CODE_API_URLS.login,
    {
      fid: accountId
    },
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
