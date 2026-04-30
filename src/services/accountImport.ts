import { createAccountsBatch, getExistingAccountIds } from '../core/accountRepository.js';
import { fetchPlayerProfile, waitForNextAccount } from './player.js';

export interface AccountImportProgressPayload {
  type: 'start' | 'progress' | 'done';
  total?: number;
  processed?: number;
  inserted?: number;
  skipped?: number;
  failed?: number;
  accountId?: string;
}

export interface AccountImportResult {
  inserted: number;
  skipped: number;
  failed: number;
  insertedAccountIds: string[];
}

export class AccountImportService {
  async importAccounts(
    accountIds: string[],
    onProgress: (payload: AccountImportProgressPayload) => void
  ): Promise<AccountImportResult> {
    const normalizedIds = Array.from(new Set(accountIds.map((item) => item.trim()).filter(Boolean)));
    const existingIds = await getExistingAccountIds(normalizedIds);
    const newIds = normalizedIds.filter((accountId) => !existingIds.has(accountId));
    const skipped = normalizedIds.length - newIds.length;
    const accountsToInsert: Array<{ accountId: string; name: string; details: Record<string, unknown> }> = [];
    let failed = 0;
    let processed = 0;

    onProgress({
      type: 'start',
      total: normalizedIds.length,
      processed: 0,
      inserted: 0,
      skipped,
      failed: 0
    });

    for (let index = 0; index < newIds.length; index += 1) {
      const accountId = newIds[index];

      try {
        const profile = await fetchPlayerProfile(accountId);
        accountsToInsert.push({
          accountId,
          name: profile.name,
          details: profile.details
        });
      } catch {
        failed += 1;
      }

      processed += 1;
      onProgress({
        type: 'progress',
        total: normalizedIds.length,
        processed: processed + skipped,
        inserted: accountsToInsert.length,
        skipped,
        failed,
        accountId
      });

      if (index < newIds.length - 1) {
        await waitForNextAccount();
      }
    }

    const result = await createAccountsBatch(accountsToInsert);
    onProgress({
      type: 'done',
      total: normalizedIds.length,
      processed: normalizedIds.length,
      inserted: result.inserted,
      skipped,
      failed
    });

    return {
      inserted: result.inserted,
      skipped,
      failed,
      insertedAccountIds: result.insertedAccountIds
    };
  }
}
