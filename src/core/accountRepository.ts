import type { Database } from 'sqlite';
import sqlite3 from 'sqlite3';
import { getDb } from './dbConnection.js';
import { ACCOUNT_STATUS, type AccountStatus, type AccountRow, type NewAccountInput } from './dbTypes.js';

function toAccountRow(row: {
  account_id: string;
  name: string;
  kid: string;
  status: number;
  is_blacklisted: number;
  is_deleted: number;
  details: string;
  sort_order: number;
  created_at: number;
  updated_at: number;
}): AccountRow {
  let details: Record<string, unknown> = {};
  try {
    details = JSON.parse(row.details || '{}') as Record<string, unknown>;
  } catch {
    details = {};
  }

  return {
    accountId: row.account_id,
    name: row.name,
    kid: row.kid,
    status: row.status as AccountStatus,
    blacklisted: row.is_blacklisted === 1,
    deleted: row.is_deleted === 1,
    details,
    sortOrder: row.sort_order,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

async function getNextSortOrder(db: Database<sqlite3.Database, sqlite3.Statement>): Promise<number> {
  const row = await db.get<{ value: number }>('SELECT COALESCE(MAX(sort_order), 0) as value FROM accounts');
  return (row?.value ?? 0) + 1;
}

function extractAccountKid(details: Record<string, unknown>): string {
  const kid = details.kid;
  if (typeof kid === 'number' && Number.isFinite(kid)) {
    return String(kid);
  }
  if (typeof kid === 'string') {
    return kid.trim();
  }
  return '';
}

export async function getExistingAccountIds(accountIds: string[]): Promise<Set<string>> {
  const normalized = Array.from(new Set(accountIds.map((id) => id.trim()).filter(Boolean)));
  if (normalized.length === 0) {
    return new Set();
  }

  const db = await getDb();
  const placeholders = normalized.map(() => '?').join(',');
  const existing = await db.all<{ account_id: string }[]>(
    `SELECT account_id FROM accounts WHERE account_id IN (${placeholders}) AND is_deleted = 0`,
    normalized
  );

  return new Set(existing.map((item) => item.account_id));
}

export async function createAccountsBatch(accounts: NewAccountInput[]): Promise<{ inserted: number; insertedAccountIds: string[] }> {
  const normalized = Array.from(
    new Map(
      accounts
        .map((account) => ({
          accountId: account.accountId.trim(),
          name: account.name.trim(),
          kid: extractAccountKid(account.details ?? {}),
          details: account.details ?? {}
        }))
        .filter((account) => account.accountId)
        .map((account) => [account.accountId, account])
    ).values()
  );
  if (normalized.length === 0) {
    return { inserted: 0, insertedAccountIds: [] };
  }

  const db = await getDb();
  await db.exec('BEGIN');
  try {
    let nextSortOrder = await getNextSortOrder(db);
    const existingRows = await db.all<{ account_id: string; is_deleted: number }[]>(
      `SELECT account_id, is_deleted FROM accounts WHERE account_id IN (${normalized.map(() => '?').join(',')})`,
      normalized.map((account) => account.accountId)
    );
    const existingMap = new Map(existingRows.map((row) => [row.account_id, row]));
    let inserted = 0;
    const insertedAccountIds: string[] = [];

    for (const account of normalized) {
      const now = Date.now();
      const existing = existingMap.get(account.accountId);

      if (existing?.is_deleted === 1) {
        await db.run(
          `UPDATE accounts
           SET name = ?, kid = ?, status = ?, is_blacklisted = 0, is_deleted = 0, details = ?, sort_order = ?, updated_at = ?
           WHERE account_id = ?`,
          account.name,
          account.kid,
          ACCOUNT_STATUS.pending,
          JSON.stringify(account.details),
          nextSortOrder,
          now,
          account.accountId
        );
        nextSortOrder += 1;
        inserted += 1;
        insertedAccountIds.push(account.accountId);
        continue;
      }

      if (existing) {
        continue;
      }

      await db.run(
        `INSERT INTO accounts (account_id, name, kid, status, is_blacklisted, is_deleted, details, sort_order, created_at, updated_at)
         VALUES (?, ?, ?, ?, 0, 0, ?, ?, ?, ?)`,
        account.accountId,
        account.name,
        account.kid,
        ACCOUNT_STATUS.pending,
        JSON.stringify(account.details),
        nextSortOrder,
        now,
        now
      );
      nextSortOrder += 1;
      inserted += 1;
      insertedAccountIds.push(account.accountId);
    }
    await db.exec('COMMIT');

    return {
      inserted,
      insertedAccountIds
    };
  } catch (error) {
    await db.exec('ROLLBACK');
    throw error;
  }
}

export async function listAccounts(): Promise<AccountRow[]> {
  const db = await getDb();
  const rows = await db.all<
    {
      account_id: string;
      name: string;
      kid: string;
      status: number;
      is_blacklisted: number;
      is_deleted: number;
      details: string;
      sort_order: number;
      created_at: number;
      updated_at: number;
    }[]
  >('SELECT * FROM accounts WHERE is_blacklisted = 0 AND is_deleted = 0 ORDER BY sort_order ASC, created_at ASC');
  return rows.map(toAccountRow);
}

export async function listBlacklistedAccounts(): Promise<AccountRow[]> {
  const db = await getDb();
  const rows = await db.all<
    {
      account_id: string;
      name: string;
      kid: string;
      status: number;
      is_blacklisted: number;
      is_deleted: number;
      details: string;
      sort_order: number;
      created_at: number;
      updated_at: number;
    }[]
  >('SELECT * FROM accounts WHERE is_blacklisted = 1 AND is_deleted = 0 ORDER BY updated_at DESC, sort_order ASC, created_at ASC');
  return rows.map(toAccountRow);
}

export async function listAccountsByStatus(status: AccountStatus): Promise<AccountRow[]> {
  const db = await getDb();
  const rows = await db.all<
    {
      account_id: string;
      name: string;
      kid: string;
      status: number;
      is_blacklisted: number;
      is_deleted: number;
      details: string;
      sort_order: number;
      created_at: number;
      updated_at: number;
    }[]
  >(
    'SELECT * FROM accounts WHERE status = ? AND is_blacklisted = 0 AND is_deleted = 0 ORDER BY sort_order ASC, created_at ASC',
    status
  );
  return rows.map(toAccountRow);
}

export async function listAccountsByIds(accountIds: string[], options?: { includeBlacklisted?: boolean }): Promise<AccountRow[]> {
  if (accountIds.length === 0) {
    return [];
  }

  const db = await getDb();
  const placeholders = accountIds.map(() => '?').join(',');
  const includeBlacklisted = options?.includeBlacklisted ?? false;
  const rows = await db.all<
    {
      account_id: string;
      name: string;
      kid: string;
      status: number;
      is_blacklisted: number;
      is_deleted: number;
      details: string;
      sort_order: number;
      created_at: number;
      updated_at: number;
    }[]
  >(
    `SELECT * FROM accounts WHERE account_id IN (${placeholders})${
      includeBlacklisted ? '' : ' AND is_blacklisted = 0'
    } AND is_deleted = 0 ORDER BY sort_order ASC, created_at ASC`,
    accountIds
  );

  return rows.map(toAccountRow);
}

export async function listAccountsByIdsIncludingDeleted(
  accountIds: string[],
  options?: { includeBlacklisted?: boolean }
): Promise<AccountRow[]> {
  if (accountIds.length === 0) {
    return [];
  }

  const db = await getDb();
  const placeholders = accountIds.map(() => '?').join(',');
  const includeBlacklisted = options?.includeBlacklisted ?? false;
  const rows = await db.all<
    {
      account_id: string;
      name: string;
      kid: string;
      status: number;
      is_blacklisted: number;
      is_deleted: number;
      details: string;
      sort_order: number;
      created_at: number;
      updated_at: number;
    }[]
  >(
    `SELECT * FROM accounts WHERE account_id IN (${placeholders})${
      includeBlacklisted ? '' : ' AND is_blacklisted = 0'
    } ORDER BY sort_order ASC, created_at ASC`,
    accountIds
  );

  return rows.map(toAccountRow);
}

export async function reorderAccounts(accountIds: string[]): Promise<void> {
  const normalized = Array.from(new Set(accountIds.map((id) => id.trim()).filter(Boolean)));
  if (normalized.length === 0) {
    return;
  }

  const db = await getDb();
  await db.exec('BEGIN');
  try {
    const now = Date.now();
    for (let index = 0; index < normalized.length; index += 1) {
      await db.run(
        'UPDATE accounts SET sort_order = ?, updated_at = ? WHERE account_id = ? AND is_deleted = 0',
        index + 1,
        now,
        normalized[index]
      );
    }
    await db.exec('COMMIT');
  } catch (error) {
    await db.exec('ROLLBACK');
    throw error;
  }
}

export async function countAccountsByStatus(status: AccountStatus): Promise<number> {
  const db = await getDb();
  const row = await db.get<{ value: number }>(
    'SELECT COUNT(*) as value FROM accounts WHERE status = ? AND is_blacklisted = 0 AND is_deleted = 0',
    status
  );
  return row?.value ?? 0;
}

export async function resetAccountsStatus(from: AccountStatus, to: AccountStatus): Promise<number> {
  const db = await getDb();
  const result = await db.run(
    'UPDATE accounts SET status = ?, updated_at = ? WHERE status = ? AND is_blacklisted = 0 AND is_deleted = 0',
    to,
    Date.now(),
    from
  );
  return result.changes ?? 0;
}

export async function updateAccountProfile(
  accountId: string,
  profile: { name: string; details: Record<string, unknown> }
): Promise<void> {
  const db = await getDb();
  await db.run(
    'UPDATE accounts SET name = ?, kid = ?, details = ?, updated_at = ? WHERE account_id = ?',
    profile.name,
    extractAccountKid(profile.details ?? {}),
    JSON.stringify(profile.details ?? {}),
    Date.now(),
    accountId
  );
}

export async function updateAccountStatus(accountId: string, status: AccountStatus): Promise<void> {
  const db = await getDb();
  await db.run(
    'UPDATE accounts SET status = ?, updated_at = ? WHERE account_id = ?',
    status,
    Date.now(),
    accountId
  );
}

export async function forceSetAllAccountsRedeemed(): Promise<number> {
  const db = await getDb();
  const result = await db.run(
    'UPDATE accounts SET status = ?, updated_at = ? WHERE is_blacklisted = 0 AND is_deleted = 0',
    ACCOUNT_STATUS.redeemed,
    Date.now()
  );
  return result.changes ?? 0;
}

export async function setAccountBlacklist(accountId: string, blacklisted: boolean): Promise<boolean> {
  const db = await getDb();
  const result = await db.run(
    'UPDATE accounts SET is_blacklisted = ?, updated_at = ? WHERE account_id = ? AND is_deleted = 0',
    blacklisted ? 1 : 0,
    Date.now(),
    accountId
  );
  return (result.changes ?? 0) > 0;
}

export async function deleteAccount(accountId: string): Promise<void> {
  const db = await getDb();
  await db.run(
    'UPDATE accounts SET is_deleted = 1, is_blacklisted = 0, updated_at = ? WHERE account_id = ? AND is_deleted = 0',
    Date.now(),
    accountId
  );
}

export async function deleteAllAccounts(): Promise<number> {
  const db = await getDb();
  const result = await db.run(
    'UPDATE accounts SET is_deleted = 1, is_blacklisted = 0, updated_at = ? WHERE is_deleted = 0',
    Date.now()
  );
  return result.changes ?? 0;
}
