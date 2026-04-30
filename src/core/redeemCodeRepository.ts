import { getDb } from './dbConnection.js';
import type { RedeemCodeInput, RedeemCodeRedemptionSummaryInput, RedeemCodeRow } from './dbTypes.js';

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
