import { getRedeemToken, setRedeemToken } from '../core/config.js';
import { fetchRemoteRedeemToken } from './redeemToken.js';

export async function ensureRedeemTokenForAutoRedeem(): Promise<void> {
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
