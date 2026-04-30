import type { Database } from 'sqlite';
import sqlite3 from 'sqlite3';

export async function initSchema(db: Database<sqlite3.Database, sqlite3.Statement>): Promise<void> {
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
