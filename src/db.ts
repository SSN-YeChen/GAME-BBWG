import fs from 'node:fs';
import path from 'node:path';
import { open, type Database } from 'sqlite';
import sqlite3 from 'sqlite3';

export const ACCOUNT_STATUS = {
  pending: 0,
  redeemed: 1,
  failed: 2
} as const;

export type AccountStatus = (typeof ACCOUNT_STATUS)[keyof typeof ACCOUNT_STATUS];

export interface AccountRow {
  accountId: string;
  name: string;
  status: AccountStatus;
  details: Record<string, unknown>;
  sortOrder: number;
  createdAt: number;
  updatedAt: number;
}

export interface NewAccountInput {
  accountId: string;
  name: string;
  details: Record<string, unknown>;
}

let dbPromise: Promise<Database<sqlite3.Database, sqlite3.Statement>> | null = null;

function getDbPath(): string {
  const dir = path.join(process.cwd(), 'data');
  fs.mkdirSync(dir, { recursive: true });
  return path.join(dir, 'app.db');
}

async function initSchema(db: Database<sqlite3.Database, sqlite3.Statement>): Promise<void> {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS accounts (
      account_id TEXT PRIMARY KEY,
      name TEXT NOT NULL DEFAULT '',
      status INTEGER NOT NULL DEFAULT 0,
      details TEXT NOT NULL DEFAULT '{}',
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);

  const columns = await db.all<{ name: string }[]>('PRAGMA table_info(accounts)');
  const hasSortOrder = columns.some((column) => column.name === 'sort_order');
  if (!hasSortOrder) {
    await db.exec('ALTER TABLE accounts ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0');
  }

  const sortStats = await db.get<{ zeroCount: number; maxSortOrder: number }>(
    'SELECT COUNT(*) FILTER (WHERE sort_order = 0) as zeroCount, COALESCE(MAX(sort_order), 0) as maxSortOrder FROM accounts'
  );
  if ((sortStats?.zeroCount ?? 0) > 0 || (sortStats?.maxSortOrder ?? 0) === 0) {
    await db.exec(`
      WITH ordered AS (
        SELECT account_id, ROW_NUMBER() OVER (ORDER BY created_at ASC, account_id ASC) AS next_sort_order
        FROM accounts
      )
      UPDATE accounts
      SET sort_order = (
        SELECT next_sort_order
        FROM ordered
        WHERE ordered.account_id = accounts.account_id
      )
      WHERE account_id IN (SELECT account_id FROM ordered);
    `);
  }
}

export async function getDb(): Promise<Database<sqlite3.Database, sqlite3.Statement>> {
  if (!dbPromise) {
    dbPromise = open({
      filename: getDbPath(),
      driver: sqlite3.Database
    });
  }

  const db = await dbPromise;
  await initSchema(db);
  return db;
}

function toAccountRow(row: {
  account_id: string;
  name: string;
  status: number;
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
    status: row.status as AccountStatus,
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

export async function getExistingAccountIds(accountIds: string[]): Promise<Set<string>> {
  const normalized = Array.from(new Set(accountIds.map((id) => id.trim()).filter(Boolean)));
  if (normalized.length === 0) {
    return new Set();
  }

  const db = await getDb();
  const placeholders = normalized.map(() => '?').join(',');
  const existing = await db.all<{ account_id: string }[]>(
    `SELECT account_id FROM accounts WHERE account_id IN (${placeholders})`,
    normalized
  );

  return new Set(existing.map((item) => item.account_id));
}

export async function createAccountsBatch(accounts: NewAccountInput[]): Promise<{ inserted: number }> {
  const normalized = Array.from(
    new Map(
      accounts
        .map((account) => ({
          accountId: account.accountId.trim(),
          name: account.name.trim(),
          details: account.details ?? {}
        }))
        .filter((account) => account.accountId)
        .map((account) => [account.accountId, account])
    ).values()
  );
  if (normalized.length === 0) {
    return { inserted: 0 };
  }

  const db = await getDb();
  await db.exec('BEGIN');
  try {
    let nextSortOrder = await getNextSortOrder(db);
    for (const account of normalized) {
      const now = Date.now();
      await db.run(
        `INSERT INTO accounts (account_id, name, status, details, sort_order, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        account.accountId,
        account.name,
        ACCOUNT_STATUS.pending,
        JSON.stringify(account.details),
        nextSortOrder,
        now,
        now
      );
      nextSortOrder += 1;
    }
    await db.exec('COMMIT');
  } catch (error) {
    await db.exec('ROLLBACK');
    throw error;
  }

  return {
    inserted: normalized.length
  };
}

export async function listAccounts(): Promise<AccountRow[]> {
  const db = await getDb();
  const rows = await db.all<
    {
      account_id: string;
      name: string;
      status: number;
      details: string;
      sort_order: number;
      created_at: number;
      updated_at: number;
    }[]
  >('SELECT * FROM accounts ORDER BY sort_order ASC, created_at ASC');
  return rows.map(toAccountRow);
}

export async function listAccountsByStatus(status: AccountStatus): Promise<AccountRow[]> {
  const db = await getDb();
  const rows = await db.all<
    {
      account_id: string;
      name: string;
      status: number;
      details: string;
      sort_order: number;
      created_at: number;
      updated_at: number;
    }[]
  >('SELECT * FROM accounts WHERE status = ? ORDER BY sort_order ASC, created_at ASC', status);
  return rows.map(toAccountRow);
}

export async function listAccountsByIds(accountIds: string[]): Promise<AccountRow[]> {
  if (accountIds.length === 0) {
    return [];
  }

  const db = await getDb();
  const placeholders = accountIds.map(() => '?').join(',');
  const rows = await db.all<
    {
      account_id: string;
      name: string;
      status: number;
      details: string;
      sort_order: number;
      created_at: number;
      updated_at: number;
    }[]
  >(`SELECT * FROM accounts WHERE account_id IN (${placeholders}) ORDER BY sort_order ASC, created_at ASC`, accountIds);

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
        'UPDATE accounts SET sort_order = ?, updated_at = ? WHERE account_id = ?',
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
  const row = await db.get<{ value: number }>('SELECT COUNT(*) as value FROM accounts WHERE status = ?', status);
  return row?.value ?? 0;
}

export async function resetAccountsStatus(from: AccountStatus, to: AccountStatus): Promise<number> {
  const db = await getDb();
  const result = await db.run(
    'UPDATE accounts SET status = ?, updated_at = ? WHERE status = ?',
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
    'UPDATE accounts SET name = ?, details = ?, updated_at = ? WHERE account_id = ?',
    profile.name,
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
    'UPDATE accounts SET status = ?, updated_at = ?',
    ACCOUNT_STATUS.redeemed,
    Date.now()
  );
  return result.changes ?? 0;
}

export async function deleteAccount(accountId: string): Promise<void> {
  const db = await getDb();
  await db.run('DELETE FROM accounts WHERE account_id = ?', accountId);
}

export async function deleteAllAccounts(): Promise<number> {
  const db = await getDb();
  const result = await db.run('DELETE FROM accounts');
  return result.changes ?? 0;
}
