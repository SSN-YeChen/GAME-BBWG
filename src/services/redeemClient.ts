import { GIFT_CODE_API_URLS, postSignedFormJson } from './giftCodeApi.js';
import type { ApiEnvelope } from './redeemTypes.js';

export function isTimeoutRetryMessage(message: string): boolean {
  return message.trim().toUpperCase() === 'TIMEOUT RETRY.';
}

export function submitLoginRequest(accountId: string, activeController: AbortController): Promise<Response> {
  return postSignedFormJson(
    GIFT_CODE_API_URLS.login,
    {
      fid: accountId
    },
    10_000,
    activeController
  );
}

export async function submitRedeemRequest(
  accountId: string,
  giftCode: string,
  activeController: AbortController
): Promise<ApiEnvelope> {
  const response = await postSignedFormJson(
    GIFT_CODE_API_URLS.redeem,
    {
      fid: accountId,
      cdk: giftCode,
      captcha_code: ''
    },
    15_000,
    activeController
  );

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  return (await response.json()) as ApiEnvelope;
}
