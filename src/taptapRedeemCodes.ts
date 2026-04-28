import { randomUUID } from 'node:crypto';
import { type RedeemCodeInput, upsertRedeemCodes } from './db.js';

const TAPTAP_TOPIC_URL = 'https://www.taptap.cn/app/759692/topic?type=official';
const TAPTAP_FEED_URL = 'https://www.taptap.cn/webapiv2/feed/v7/by-group';
const TAPTAP_GROUP_ID = '974349';
const POLL_INTERVAL_MS = 60_000;

interface TapTapFeedResponse {
  data?: {
    list?: TapTapFeedItem[];
  };
  success?: boolean;
}

interface TapTapFeedItem {
  type?: string;
  identification?: string;
  moment?: {
    id_str?: string;
    publish_time?: number;
    created_time?: number;
    topic?: {
      id_str?: string;
      title?: string;
      summary?: string;
    };
    sharing?: {
      url?: string;
      title?: string;
      description?: string;
    };
  };
}

export interface TapTapRedeemCodePollResult {
  found: number;
  inserted: number;
  updated: number;
  insertedCodes: string[];
}

let pollTimer: NodeJS.Timeout | null = null;
let pollInFlight = false;
let pollingPaused = false;

function formatLogTime(): string {
  return new Date().toLocaleString('zh-CN', { hour12: false });
}

function buildTapTapFeedUrl(): string {
  const url = new URL(TAPTAP_FEED_URL);
  const xua = new URLSearchParams({
    V: '1',
    PN: 'WebApp',
    LANG: 'zh_CN',
    VN_CODE: '102',
    LOC: 'CN',
    PLT: 'PC',
    DS: 'Android',
    UID: randomUUID(),
    DT: 'PC'
  });

  url.searchParams.set('X-UA', xua.toString());
  url.searchParams.set('type', 'official');
  url.searchParams.set('group_id', TAPTAP_GROUP_ID);
  url.searchParams.set('sort', 'commented');
  url.searchParams.set('limit', '10');
  url.searchParams.set('__times', '0');
  url.searchParams.set('status', '0');
  url.searchParams.set('with_hot_comment', 'true');
  return url.toString();
}

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, ' ').trim();
}

function extractRedeemCodes(...texts: string[]): string[] {
  const codes = new Set<string>();
  const sourceText = texts.filter(Boolean).join('\n');
  const explicitCodePattern = /(?:兑换码|礼包码|CDK|cdk)\s*[：:：\s]\s*([A-Za-z0-9][A-Za-z0-9_-]{2,31})/gu;

  for (const match of sourceText.matchAll(explicitCodePattern)) {
    const code = match[1]?.replace(/[^A-Za-z0-9_-]/g, '').trim();
    if (code) {
      codes.add(code.toUpperCase());
    }
  }

  return Array.from(codes);
}

function toRedeemCodeInputs(item: TapTapFeedItem): RedeemCodeInput[] {
  const moment = item.moment;
  if (!moment) {
    return [];
  }

  const title = moment.topic?.title || moment.sharing?.title || '';
  const summary = moment.topic?.summary || moment.sharing?.description || '';
  const content = [title, summary].filter(Boolean).join('\n');
  const codes = extractRedeemCodes(title, summary, moment.sharing?.description || '');
  const sourceId = moment.id_str || moment.topic?.id_str || item.identification || '';
  const sourceUrl = moment.sharing?.url || (sourceId ? `https://www.taptap.cn/moment/${sourceId}` : TAPTAP_TOPIC_URL);
  const publishedAtSeconds = moment.publish_time || moment.created_time || 0;

  return codes.map((code) => ({
    code,
    sourceId,
    sourceUrl,
    title,
    summary: normalizeWhitespace(summary),
    content,
    publishedAt: publishedAtSeconds > 0 ? publishedAtSeconds * 1000 : 0
  }));
}

export async function fetchTapTapRedeemCodes(): Promise<RedeemCodeInput[]> {
  const response = await fetch(buildTapTapFeedUrl(), {
    headers: {
      'User-Agent': 'Mozilla/5.0 (compatible; bb-web/1.0)',
      Referer: TAPTAP_TOPIC_URL,
      Accept: 'application/json'
    }
  });

  if (!response.ok) {
    throw new Error(`TapTap 兑换码列表请求失败: HTTP ${response.status}`);
  }

  const payload = (await response.json()) as TapTapFeedResponse;
  const list = payload.data?.list;
  if (!Array.isArray(list)) {
    throw new Error('TapTap 兑换码列表响应格式异常。');
  }

  return Array.from(new Map(list.flatMap(toRedeemCodeInputs).map((item) => [item.code, item])).values());
}

export async function pollTapTapRedeemCodes(): Promise<TapTapRedeemCodePollResult> {
  const codes = await fetchTapTapRedeemCodes();
  const result = await upsertRedeemCodes(codes);
  return {
    found: codes.length,
    inserted: result.inserted,
    updated: result.updated,
    insertedCodes: result.insertedCodes
  };
}

export function startTapTapRedeemCodePolling(options?: { onNewCodes?: (codes: string[]) => void | Promise<void> }): void {
  if (pollTimer) {
    return;
  }

  const runPoll = async () => {
    if (pollingPaused) {
      return;
    }

    if (pollInFlight) {
      return;
    }

    pollInFlight = true;
    try {
      const result = await pollTapTapRedeemCodes();
      // eslint-disable-next-line no-console
      console.log(`[${formatLogTime()}] TapTap 自动抓取兑换码：成功`);
      if (result.inserted > 0) {
        await options?.onNewCodes?.(result.insertedCodes);
      }
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error(`[${formatLogTime()}] TapTap 自动抓取兑换码：失败`, error);
    } finally {
      pollInFlight = false;
    }
  };

  void runPoll();
  pollTimer = setInterval(() => {
    void runPoll();
  }, POLL_INTERVAL_MS);
}

export function pauseTapTapRedeemCodePolling(): void {
  pollingPaused = true;
}

export function resumeTapTapRedeemCodePolling(): void {
  pollingPaused = false;
}
