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
  kid: string;
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

export interface RedeemCodeInput {
  code: string;
  sourceId: string;
  sourceUrl: string;
  title: string;
  summary: string;
  content: string;
  publishedAt: number;
}

export interface RedeemCodeRow extends RedeemCodeInput {
  firstSeenAt: number;
  lastSeenAt: number;
  autoRedeemStatus?: RedeemCodeRedemptionStatus;
  autoRedeemStartedAt?: number;
  autoRedeemCompletedAt?: number;
  autoRedeemLastError?: string;
}

export type RedeemCodeRedemptionStatus = 'running' | 'completed' | 'failed';

export interface RedeemCodeRedemptionSummaryInput {
  total: number;
  processed: number;
  successCount: number;
  receivedCount: number;
  failureCount: number;
  remaining: number;
}

export interface WechatArticleInput {
  aid: string;
  title: string;
  link: string;
  author: string;
  fakeid: string;
  digest: string;
  cover: string;
  publishedAt: number;
  updatedAt: number;
}

export interface WechatArticleDetailInput {
  aid: string;
  html: string;
  text: string;
  fetchStatus: 'ok' | 'failed';
  fetchError: string;
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
      kid TEXT NOT NULL DEFAULT '',
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

  await db.exec(`
    CREATE TABLE IF NOT EXISTS redeem_codes (
      code TEXT PRIMARY KEY,
      source_id TEXT NOT NULL DEFAULT '',
      source_url TEXT NOT NULL DEFAULT '',
      title TEXT NOT NULL DEFAULT '',
      summary TEXT NOT NULL DEFAULT '',
      content TEXT NOT NULL DEFAULT '',
      published_at INTEGER NOT NULL DEFAULT 0,
      first_seen_at INTEGER NOT NULL,
      last_seen_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_redeem_codes_last_seen_at ON redeem_codes(last_seen_at DESC);
    CREATE INDEX IF NOT EXISTS idx_redeem_codes_published_at ON redeem_codes(published_at DESC);
  `);

  await db.exec(`
    CREATE TABLE IF NOT EXISTS redeem_code_redemptions (
      code TEXT PRIMARY KEY,
      status TEXT NOT NULL DEFAULT 'running',
      total INTEGER NOT NULL DEFAULT 0,
      processed INTEGER NOT NULL DEFAULT 0,
      success_count INTEGER NOT NULL DEFAULT 0,
      received_count INTEGER NOT NULL DEFAULT 0,
      failure_count INTEGER NOT NULL DEFAULT 0,
      remaining INTEGER NOT NULL DEFAULT 0,
      started_at INTEGER NOT NULL,
      completed_at INTEGER NOT NULL DEFAULT 0,
      last_error TEXT NOT NULL DEFAULT '',
      updated_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_redeem_code_redemptions_status ON redeem_code_redemptions(status);
    CREATE INDEX IF NOT EXISTS idx_redeem_code_redemptions_updated_at ON redeem_code_redemptions(updated_at DESC);
  `);

  await db.exec(`
    CREATE TABLE IF NOT EXISTS wechat_articles (
      aid TEXT PRIMARY KEY,
      title TEXT NOT NULL DEFAULT '',
      link TEXT NOT NULL DEFAULT '',
      author TEXT NOT NULL DEFAULT '',
      fakeid TEXT NOT NULL DEFAULT '',
      digest TEXT NOT NULL DEFAULT '',
      cover TEXT NOT NULL DEFAULT '',
      html TEXT NOT NULL DEFAULT '',
      text TEXT NOT NULL DEFAULT '',
      fetch_status TEXT NOT NULL DEFAULT 'pending',
      fetch_error TEXT NOT NULL DEFAULT '',
      published_at INTEGER NOT NULL DEFAULT 0,
      source_updated_at INTEGER NOT NULL DEFAULT 0,
      first_seen_at INTEGER NOT NULL,
      last_seen_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_wechat_articles_published_at ON wechat_articles(published_at DESC);
    CREATE INDEX IF NOT EXISTS idx_wechat_articles_last_seen_at ON wechat_articles(last_seen_at DESC);
    CREATE INDEX IF NOT EXISTS idx_wechat_articles_fakeid ON wechat_articles(fakeid);
  `);
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

export async function upsertWechatArticles(articles: WechatArticleInput[]): Promise<{ insertedAids: string[]; updated: number }> {
  const normalized = Array.from(
    new Map(
      articles
        .map((article) => ({
          ...article,
          aid: article.aid.trim(),
          title: article.title.trim(),
          link: article.link.trim(),
          author: article.author.trim(),
          fakeid: article.fakeid.trim(),
          digest: article.digest.trim(),
          cover: article.cover.trim()
        }))
        .filter((article) => article.aid && article.link)
        .map((article) => [article.aid, article])
    ).values()
  );

  if (normalized.length === 0) {
    return { insertedAids: [], updated: 0 };
  }

  const db = await getDb();
  await db.exec('BEGIN');
  try {
    const now = Date.now();
    const insertedAids: string[] = [];
    let updated = 0;

    for (const article of normalized) {
      const existing = await db.get<{ aid: string }>('SELECT aid FROM wechat_articles WHERE aid = ?', article.aid);
      await db.run(
        `INSERT INTO wechat_articles (
          aid,
          title,
          link,
          author,
          fakeid,
          digest,
          cover,
          published_at,
          source_updated_at,
          first_seen_at,
          last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(aid) DO UPDATE SET
          title = excluded.title,
          link = excluded.link,
          author = excluded.author,
          fakeid = excluded.fakeid,
          digest = excluded.digest,
          cover = excluded.cover,
          published_at = excluded.published_at,
          source_updated_at = excluded.source_updated_at,
          last_seen_at = excluded.last_seen_at`,
        article.aid,
        article.title,
        article.link,
        article.author,
        article.fakeid,
        article.digest,
        article.cover,
        article.publishedAt,
        article.updatedAt,
        now,
        now
      );

      if (existing) {
        updated += 1;
      } else {
        insertedAids.push(article.aid);
      }
    }

    await db.exec('COMMIT');
    return { insertedAids, updated };
  } catch (error) {
    await db.exec('ROLLBACK');
    throw error;
  }
}

export async function updateWechatArticleDetail(input: WechatArticleDetailInput): Promise<void> {
  const db = await getDb();
  await db.run(
    `UPDATE wechat_articles
     SET html = ?,
         text = ?,
         fetch_status = ?,
         fetch_error = ?,
         last_seen_at = ?
     WHERE aid = ?`,
    input.html,
    input.text,
    input.fetchStatus,
    input.fetchError,
    Date.now(),
    input.aid.trim()
  );
}

export async function listWechatArticlesByAids(aids: string[]): Promise<WechatArticleInput[]> {
  const normalized = Array.from(new Set(aids.map((aid) => aid.trim()).filter(Boolean)));
  if (normalized.length === 0) {
    return [];
  }

  const db = await getDb();
  const placeholders = normalized.map(() => '?').join(',');
  const rows = await db.all<
    {
      aid: string;
      title: string;
      link: string;
      author: string;
      fakeid: string;
      digest: string;
      cover: string;
      published_at: number;
      source_updated_at: number;
    }[]
  >(`SELECT * FROM wechat_articles WHERE aid IN (${placeholders})`, normalized);

  return rows.map((row) => ({
    aid: row.aid,
    title: row.title,
    link: row.link,
    author: row.author,
    fakeid: row.fakeid,
    digest: row.digest,
    cover: row.cover,
    publishedAt: row.published_at,
    updatedAt: row.source_updated_at
  }));
}

function toRedeemCodeRow(row: {
  code: string;
  source_id: string;
  source_url: string;
  title: string;
  summary: string;
  content: string;
  published_at: number;
  first_seen_at: number;
  last_seen_at: number;
  auto_redeem_status?: string | null;
  auto_redeem_started_at?: number | null;
  auto_redeem_completed_at?: number | null;
  auto_redeem_last_error?: string | null;
}): RedeemCodeRow {
  return {
    code: row.code,
    sourceId: row.source_id,
    sourceUrl: row.source_url,
    title: row.title,
    summary: row.summary,
    content: row.content,
    publishedAt: row.published_at,
    firstSeenAt: row.first_seen_at,
    lastSeenAt: row.last_seen_at,
    autoRedeemStatus:
      row.auto_redeem_status === 'running' || row.auto_redeem_status === 'completed' || row.auto_redeem_status === 'failed'
        ? row.auto_redeem_status
        : undefined,
    autoRedeemStartedAt: row.auto_redeem_started_at ?? undefined,
    autoRedeemCompletedAt: row.auto_redeem_completed_at ?? undefined,
    autoRedeemLastError: row.auto_redeem_last_error ?? undefined
  };
}

export async function upsertRedeemCodes(
  codes: RedeemCodeInput[]
): Promise<{ inserted: number; updated: number; insertedCodes: string[] }> {
  const normalized = Array.from(
    new Map(
      codes
        .map((item) => ({
          ...item,
          code: item.code.trim().toUpperCase(),
          sourceId: item.sourceId.trim(),
          sourceUrl: item.sourceUrl.trim(),
          title: item.title.trim(),
          summary: item.summary.trim(),
          content: item.content.trim()
        }))
        .filter((item) => item.code)
        .map((item) => [item.code, item])
    ).values()
  );

  if (normalized.length === 0) {
    return { inserted: 0, updated: 0, insertedCodes: [] };
  }

  const db = await getDb();
  await db.exec('BEGIN');
  try {
    let inserted = 0;
    let updated = 0;
    const insertedCodes: string[] = [];
    const now = Date.now();

    for (const item of normalized) {
      const existing = await db.get<{ code: string }>('SELECT code FROM redeem_codes WHERE code = ?', item.code);
      await db.run(
        `INSERT INTO redeem_codes (
          code,
          source_id,
          source_url,
          title,
          summary,
          content,
          published_at,
          first_seen_at,
          last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
          source_id = excluded.source_id,
          source_url = excluded.source_url,
          title = excluded.title,
          summary = excluded.summary,
          content = excluded.content,
          published_at = excluded.published_at,
          last_seen_at = excluded.last_seen_at`,
        item.code,
        item.sourceId,
        item.sourceUrl,
        item.title,
        item.summary,
        item.content,
        item.publishedAt,
        now,
        now
      );

      if (existing) {
        updated += 1;
      } else {
        inserted += 1;
        insertedCodes.push(item.code);
      }
    }

    await db.exec('COMMIT');
    return { inserted, updated, insertedCodes };
  } catch (error) {
    await db.exec('ROLLBACK');
    throw error;
  }
}

export async function listRedeemCodes(limit = 50): Promise<RedeemCodeRow[]> {
  const db = await getDb();
  const rows = await db.all<
    {
      code: string;
      source_id: string;
      source_url: string;
      title: string;
      summary: string;
      content: string;
      published_at: number;
      first_seen_at: number;
      last_seen_at: number;
      auto_redeem_status?: string | null;
      auto_redeem_started_at?: number | null;
      auto_redeem_completed_at?: number | null;
      auto_redeem_last_error?: string | null;
    }[]
  >(
    `SELECT
       redeem_codes.*,
       redeem_code_redemptions.status AS auto_redeem_status,
       redeem_code_redemptions.started_at AS auto_redeem_started_at,
       redeem_code_redemptions.completed_at AS auto_redeem_completed_at,
       redeem_code_redemptions.last_error AS auto_redeem_last_error
     FROM redeem_codes
     LEFT JOIN redeem_code_redemptions ON redeem_code_redemptions.code = redeem_codes.code
     ORDER BY redeem_codes.published_at DESC, redeem_codes.last_seen_at DESC
     LIMIT ?`,
    Math.max(1, Math.min(limit, 200))
  );

  return rows.map(toRedeemCodeRow);
}

export async function reserveRedeemCodeRedemption(code: string): Promise<boolean> {
  const normalizedCode = code.trim().toUpperCase();
  if (!normalizedCode) {
    return false;
  }

  const db = await getDb();
  const now = Date.now();
  const result = await db.run(
    `INSERT OR IGNORE INTO redeem_code_redemptions (
      code,
      status,
      started_at,
      updated_at
    ) VALUES (?, 'running', ?, ?)`,
    normalizedCode,
    now,
    now
  );

  return (result.changes ?? 0) > 0;
}

export async function completeRedeemCodeRedemption(
  code: string,
  summary: RedeemCodeRedemptionSummaryInput
): Promise<void> {
  const normalizedCode = code.trim().toUpperCase();
  if (!normalizedCode) {
    return;
  }

  const db = await getDb();
  const now = Date.now();
  await db.run(
    `UPDATE redeem_code_redemptions
     SET status = 'completed',
         total = ?,
         processed = ?,
         success_count = ?,
         received_count = ?,
         failure_count = ?,
         remaining = ?,
         completed_at = ?,
         last_error = '',
         updated_at = ?
     WHERE code = ?`,
    summary.total,
    summary.processed,
    summary.successCount,
    summary.receivedCount,
    summary.failureCount,
    summary.remaining,
    now,
    now,
    normalizedCode
  );
}

export async function failRedeemCodeRedemption(code: string, error: string): Promise<void> {
  const normalizedCode = code.trim().toUpperCase();
  if (!normalizedCode) {
    return;
  }

  const db = await getDb();
  const now = Date.now();
  await db.run(
    `UPDATE redeem_code_redemptions
     SET status = 'failed',
         completed_at = ?,
         last_error = ?,
         updated_at = ?
     WHERE code = ?`,
    now,
    error.trim(),
    now,
    normalizedCode
  );
}
