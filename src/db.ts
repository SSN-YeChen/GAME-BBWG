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
  blacklisted: boolean;
  deleted: boolean;
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

export interface VisitorLogInput {
  ipAddress: string;
  method: string;
  protocol: string;
  host: string;
  path: string;
  query: string;
  params: string;
  headers: string;
  body: string;
  statusCode: number;
  durationMs: number;
  username: string;
  userRole: string;
  userAgent: string;
  referer: string;
  cfRay: string;
  cfCountry: string;
  blocked: boolean;
  blockReason: string;
  createdAt: number;
}

export interface VisitorLogRow extends VisitorLogInput {
  id: number;
}

export interface BlacklistEntry {
  ipAddress: string;
  reason: string;
  createdAt: number;
  updatedAt: number;
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
      is_blacklisted INTEGER NOT NULL DEFAULT 0,
      is_deleted INTEGER NOT NULL DEFAULT 0,
      details TEXT NOT NULL DEFAULT '{}',
      sort_order INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
  `);

  await db.exec(`
    CREATE TABLE IF NOT EXISTS visitor_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ip_address TEXT NOT NULL DEFAULT '',
      method TEXT NOT NULL DEFAULT '',
      protocol TEXT NOT NULL DEFAULT '',
      host TEXT NOT NULL DEFAULT '',
      path TEXT NOT NULL DEFAULT '',
      query TEXT NOT NULL DEFAULT '',
      params TEXT NOT NULL DEFAULT '',
      headers TEXT NOT NULL DEFAULT '',
      body TEXT NOT NULL DEFAULT '',
      status_code INTEGER NOT NULL DEFAULT 0,
      duration_ms INTEGER NOT NULL DEFAULT 0,
      username TEXT NOT NULL DEFAULT '',
      user_role TEXT NOT NULL DEFAULT '',
      user_agent TEXT NOT NULL DEFAULT '',
      referer TEXT NOT NULL DEFAULT '',
      cf_ray TEXT NOT NULL DEFAULT '',
      cf_country TEXT NOT NULL DEFAULT '',
      blocked INTEGER NOT NULL DEFAULT 0,
      block_reason TEXT NOT NULL DEFAULT '',
      created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_visitor_logs_created_at ON visitor_logs(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_visitor_logs_ip_address ON visitor_logs(ip_address);
    CREATE INDEX IF NOT EXISTS idx_visitor_logs_path ON visitor_logs(path);
    CREATE INDEX IF NOT EXISTS idx_visitor_logs_status_code ON visitor_logs(status_code);
  `);

  await db.exec(`
    CREATE TABLE IF NOT EXISTS visitor_blacklist (
      ip_address TEXT PRIMARY KEY,
      reason TEXT NOT NULL DEFAULT '',
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_visitor_blacklist_updated_at ON visitor_blacklist(updated_at DESC);
  `);

  const visitorLogColumns = await db.all<{ name: string }[]>('PRAGMA table_info(visitor_logs)');
  const hasCfCountry = visitorLogColumns.some((column) => column.name === 'cf_country');
  if (!hasCfCountry) {
    await db.exec("ALTER TABLE visitor_logs ADD COLUMN cf_country TEXT NOT NULL DEFAULT ''");
  }
  const hasBlocked = visitorLogColumns.some((column) => column.name === 'blocked');
  if (!hasBlocked) {
    await db.exec('ALTER TABLE visitor_logs ADD COLUMN blocked INTEGER NOT NULL DEFAULT 0');
  }
  const hasBlockReason = visitorLogColumns.some((column) => column.name === 'block_reason');
  if (!hasBlockReason) {
    await db.exec("ALTER TABLE visitor_logs ADD COLUMN block_reason TEXT NOT NULL DEFAULT ''");
  }

  const columns = await db.all<{ name: string }[]>('PRAGMA table_info(accounts)');
  const hasSortOrder = columns.some((column) => column.name === 'sort_order');
  if (!hasSortOrder) {
    await db.exec('ALTER TABLE accounts ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0');
  }
  const hasBlacklisted = columns.some((column) => column.name === 'is_blacklisted');
  if (!hasBlacklisted) {
    await db.exec('ALTER TABLE accounts ADD COLUMN is_blacklisted INTEGER NOT NULL DEFAULT 0');
  }
  const hasDeleted = columns.some((column) => column.name === 'is_deleted');
  if (!hasDeleted) {
    await db.exec('ALTER TABLE accounts ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0');
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
    const existingRows = await db.all<{ account_id: string; is_deleted: number }[]>(
      `SELECT account_id, is_deleted FROM accounts WHERE account_id IN (${normalized.map(() => '?').join(',')})`,
      normalized.map((account) => account.accountId)
    );
    const existingMap = new Map(existingRows.map((row) => [row.account_id, row]));
    let inserted = 0;

    for (const account of normalized) {
      const now = Date.now();
      const existing = existingMap.get(account.accountId);

      if (existing?.is_deleted === 1) {
        await db.run(
          `UPDATE accounts
           SET name = ?, status = ?, is_blacklisted = 0, is_deleted = 0, details = ?, sort_order = ?, updated_at = ?
           WHERE account_id = ?`,
          account.name,
          ACCOUNT_STATUS.pending,
          JSON.stringify(account.details),
          nextSortOrder,
          now,
          account.accountId
        );
        nextSortOrder += 1;
        inserted += 1;
        continue;
      }

      if (existing) {
        continue;
      }

      await db.run(
        `INSERT INTO accounts (account_id, name, status, is_blacklisted, is_deleted, details, sort_order, created_at, updated_at)
         VALUES (?, ?, ?, 0, 0, ?, ?, ?, ?)`,
        account.accountId,
        account.name,
        ACCOUNT_STATUS.pending,
        JSON.stringify(account.details),
        nextSortOrder,
        now,
        now
      );
      nextSortOrder += 1;
      inserted += 1;
    }
    await db.exec('COMMIT');

    return {
      inserted
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

export async function createVisitorLog(input: VisitorLogInput): Promise<void> {
  const db = await getDb();
  await db.run(
    `INSERT INTO visitor_logs (
      ip_address,
      method,
      protocol,
      host,
      path,
      query,
      params,
      headers,
      body,
      status_code,
      duration_ms,
      username,
      user_role,
      user_agent,
      referer,
      cf_ray,
      cf_country,
      blocked,
      block_reason,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    input.ipAddress,
    input.method,
    input.protocol,
    input.host,
    input.path,
    input.query,
    input.params,
    input.headers,
    input.body,
    input.statusCode,
    input.durationMs,
    input.username,
    input.userRole,
    input.userAgent,
    input.referer,
    input.cfRay,
    input.cfCountry,
    input.blocked ? 1 : 0,
    input.blockReason,
    input.createdAt
  );
}

export async function cleanupVisitorLogs(retentionDays = 30): Promise<number> {
  const db = await getDb();
  const threshold = Date.now() - retentionDays * 24 * 60 * 60 * 1000;
  const result = await db.run('DELETE FROM visitor_logs WHERE created_at < ?', threshold);
  return result.changes ?? 0;
}

export async function deleteAllVisitorLogs(): Promise<number> {
  const db = await getDb();
  const result = await db.run('DELETE FROM visitor_logs');
  return result.changes ?? 0;
}

export async function listVisitorLogs(limit = 100): Promise<VisitorLogRow[]> {
  const db = await getDb();
  const rows = await db.all<
    {
      id: number;
      ip_address: string;
      method: string;
      protocol: string;
      host: string;
      path: string;
      query: string;
      params: string;
      headers: string;
      body: string;
      status_code: number;
      duration_ms: number;
      username: string;
      user_role: string;
      user_agent: string;
      referer: string;
      cf_ray: string;
      cf_country: string;
      blocked: number;
      block_reason: string;
      created_at: number;
    }[]
  >('SELECT * FROM visitor_logs ORDER BY id DESC LIMIT ?', limit);

  return rows.map((row) => ({
    id: row.id,
    ipAddress: row.ip_address,
    method: row.method,
    protocol: row.protocol,
    host: row.host,
    path: row.path,
    query: row.query,
    params: row.params,
    headers: row.headers,
    body: row.body,
    statusCode: row.status_code,
    durationMs: row.duration_ms,
    username: row.username,
    userRole: row.user_role,
    userAgent: row.user_agent,
    referer: row.referer,
    cfRay: row.cf_ray,
    cfCountry: row.cf_country,
    blocked: row.blocked === 1,
    blockReason: row.block_reason,
    createdAt: row.created_at
  }));
}

export async function listBlacklistEntries(): Promise<BlacklistEntry[]> {
  const db = await getDb();
  const rows = await db.all<
    {
      ip_address: string;
      reason: string;
      created_at: number;
      updated_at: number;
    }[]
  >('SELECT * FROM visitor_blacklist ORDER BY updated_at DESC, ip_address ASC');

  return rows.map((row) => ({
    ipAddress: row.ip_address,
    reason: row.reason,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  }));
}

export async function getBlacklistEntry(ipAddress: string): Promise<BlacklistEntry | null> {
  const db = await getDb();
  const row = await db.get<{
    ip_address: string;
    reason: string;
    created_at: number;
    updated_at: number;
  }>('SELECT * FROM visitor_blacklist WHERE ip_address = ?', ipAddress);

  if (!row) {
    return null;
  }

  return {
    ipAddress: row.ip_address,
    reason: row.reason,
    createdAt: row.created_at,
    updatedAt: row.updated_at
  };
}

export async function upsertBlacklistEntry(ipAddress: string, reason: string): Promise<void> {
  const db = await getDb();
  const now = Date.now();
  await db.run(
    `INSERT INTO visitor_blacklist (ip_address, reason, created_at, updated_at)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(ip_address) DO UPDATE SET reason = excluded.reason, updated_at = excluded.updated_at`,
    ipAddress,
    reason,
    now,
    now
  );
}

export async function deleteBlacklistEntry(ipAddress: string): Promise<void> {
  const db = await getDb();
  await db.run('DELETE FROM visitor_blacklist WHERE ip_address = ?', ipAddress);
}
