import {
  ACCOUNT_STATUS,
  countAccountsByStatus,
  listAccounts,
  listAccountsByIds,
  listAccountsByStatus,
  resetAccountsStatus,
  type AccountRow
} from '../core/db.js';
import type { RedeemRunOptions } from './redeemTypes.js';

export async function selectRedeemAccounts(
  targetAccountIds: string[] | undefined,
  options: RedeemRunOptions | undefined
): Promise<{ accounts: AccountRow[]; resetTriggered: boolean }> {
  const includeAllAccounts = options?.includeAllAccounts ?? false;
  const includeTargetAccounts = options?.includeTargetAccounts ?? false;
  let accounts = includeAllAccounts
    ? await listAccounts()
    : targetAccountIds && targetAccountIds.length > 0
      ? includeTargetAccounts
        ? await listAccountsByIds(targetAccountIds)
        : (await listAccountsByIds(targetAccountIds)).filter((item) => item.status === ACCOUNT_STATUS.failed)
      : await listAccountsByStatus(ACCOUNT_STATUS.pending);

  if (!includeAllAccounts && (!targetAccountIds || targetAccountIds.length === 0) && accounts.length === 0) {
    await resetAccountsStatus(ACCOUNT_STATUS.redeemed, ACCOUNT_STATUS.pending);
    accounts = await listAccountsByStatus(ACCOUNT_STATUS.pending);
    return { accounts, resetTriggered: true };
  }

  return { accounts, resetTriggered: false };
}

export async function countRemainingRedeemAccounts(options: RedeemRunOptions | undefined): Promise<number> {
  if (options?.includeAllAccounts || options?.includeTargetAccounts) {
    return 0;
  }

  return countAccountsByStatus(ACCOUNT_STATUS.pending);
}
