import { getDb } from './dbConnection.js';
import type { BlacklistEntry, VisitorLogInput, VisitorLogRow } from './dbTypes.js';

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
