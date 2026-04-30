import { getDb } from './dbConnection.js';
import type { WechatArticleDetailInput, WechatArticleInput } from './dbTypes.js';

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
