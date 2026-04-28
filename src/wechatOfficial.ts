import {
  type RedeemCodeInput,
  type WechatArticleInput,
  listWechatArticlesByAids,
  updateWechatArticleDetail,
  upsertRedeemCodes,
  upsertWechatArticles
} from './db.js';
import { getWechatMpConfig } from './config.js';

const WECHAT_APPMSG_PUBLISH_URL = 'https://mp.weixin.qq.com/cgi-bin/appmsgpublish';
const POLL_INTERVAL_MS = 60_000;
const WECHAT_ARTICLE_USER_AGENT =
  'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 MicroMessenger/8.0.34(0x16082222) NetType/WIFI Language/zh_CN';

interface WechatBaseResponse {
  ret?: number;
  err_msg?: string;
}

interface WechatArticleListResponse {
  base_resp?: WechatBaseResponse;
  publish_page?: string;
}

interface WechatPublishPage {
  total_count?: number;
  publish_list?: Array<{
    publish_info?: string;
  }>;
}

interface WechatPublishInfo {
  sent_info?: {
    time?: number;
  };
  appmsgex?: Array<{
    aid?: string;
    title?: string;
    link?: string;
    digest?: string;
    cover?: string;
    author_name?: string;
    create_time?: number;
    update_time?: number;
  }>;
}

export interface WechatRedeemCodePollResult {
  foundArticles: number;
  insertedArticles: number;
  foundCodes: number;
  inserted: number;
  updated: number;
  insertedCodes: string[];
}

let pollTimer: NodeJS.Timeout | null = null;
let pollInFlight = false;
let pollingPaused = false;
let disabledByInvalidSession = false;

function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, ' ').trim();
}

function stripHtmlToText(html: string): string {
  return normalizeWhitespace(
    html
      .replace(/<script\b[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style\b[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&lt;/gi, '<')
      .replace(/&gt;/gi, '>')
      .replace(/&quot;/gi, '"')
      .replace(/&#39;/gi, "'")
      .replace(/&#x27;/gi, "'")
  );
}

function extractArticleText(html: string): string {
  const contentMatch = html.match(/<div[^>]+id=["']js_content["'][^>]*>([\s\S]*?)<\/div>\s*<script/i);
  if (contentMatch?.[1]) {
    return stripHtmlToText(contentMatch[1]);
  }
  return stripHtmlToText(html);
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

function parseJsonString<T>(value: string | undefined, fallback: T): T {
  if (!value) {
    return fallback;
  }

  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function buildArticleListUrl(begin = 0, count = 10): string {
  const config = getWechatMpConfig();
  const url = new URL(WECHAT_APPMSG_PUBLISH_URL);
  url.searchParams.set('sub', 'list');
  url.searchParams.set('search_field', 'null');
  url.searchParams.set('begin', begin.toString());
  url.searchParams.set('count', count.toString());
  url.searchParams.set('query', '');
  url.searchParams.set('fakeid', config.fakeid);
  url.searchParams.set('type', '101_1');
  url.searchParams.set('free_publish_type', '1');
  url.searchParams.set('sub_action', 'list_ex');
  url.searchParams.set('token', config.token);
  url.searchParams.set('lang', 'zh_CN');
  url.searchParams.set('f', 'json');
  url.searchParams.set('ajax', '1');
  return url.toString();
}

async function fetchWechatArticleList(): Promise<WechatArticleInput[]> {
  const config = getWechatMpConfig();
  if (!config.token || !config.cookie) {
    throw new Error('微信模式缺少 WECHAT_MP_TOKEN 或 WECHAT_MP_COOKIE。');
  }

  const response = await fetch(buildArticleListUrl(), {
    headers: {
      Accept: 'application/json, text/plain, */*',
      Cookie: config.cookie,
      'User-Agent': config.userAgent,
      'X-Requested-With': 'XMLHttpRequest',
      Referer: 'https://mp.weixin.qq.com/'
    }
  });

  if (!response.ok) {
    throw new Error(`微信文章列表请求失败: HTTP ${response.status}`);
  }

  const payload = (await response.json()) as WechatArticleListResponse;
  const ret = payload.base_resp?.ret;
  if (ret !== 0) {
    const message = payload.base_resp?.err_msg || '未知错误';
    if (ret === 200003 || message.includes('invalid session')) {
      disabledByInvalidSession = true;
      throw new Error(`微信登录态已失效: ${message}`);
    }
    throw new Error(`微信文章列表响应异常: ret=${ret}, msg=${message}`);
  }

  const publishPage = parseJsonString<WechatPublishPage>(payload.publish_page, {});
  const articles: WechatArticleInput[] = [];

  for (const item of publishPage.publish_list ?? []) {
    const publishInfo = parseJsonString<WechatPublishInfo>(item.publish_info, {});
    for (const article of publishInfo.appmsgex ?? []) {
      const aid = article.aid?.trim() ?? '';
      const link = article.link?.trim() ?? '';
      if (!aid || !link) {
        continue;
      }

      const publishedAtSeconds = article.create_time || publishInfo.sent_info?.time || article.update_time || 0;
      articles.push({
        aid,
        title: article.title ?? '',
        link,
        author: article.author_name ?? '',
        fakeid: config.fakeid,
        digest: article.digest ?? '',
        cover: article.cover ?? '',
        publishedAt: publishedAtSeconds > 0 ? publishedAtSeconds * 1000 : 0,
        updatedAt: (article.update_time || publishedAtSeconds) * 1000
      });
    }
  }

  return articles;
}

async function fetchWechatArticleHtml(url: string): Promise<string> {
  const response = await fetch(url, {
    redirect: 'follow',
    headers: {
      'User-Agent': WECHAT_ARTICLE_USER_AGENT,
      Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
  });

  if (!response.ok) {
    throw new Error(`微信文章详情请求失败: HTTP ${response.status}`);
  }

  return response.text();
}

async function fetchNewArticleDetails(aids: string[]): Promise<RedeemCodeInput[]> {
  const articles = await listWechatArticlesByAids(aids);
  const redeemCodes: RedeemCodeInput[] = [];

  for (const article of articles) {
    try {
      const html = await fetchWechatArticleHtml(article.link);
      const text = extractArticleText(html);
      await updateWechatArticleDetail({
        aid: article.aid,
        html,
        text,
        fetchStatus: 'ok',
        fetchError: ''
      });

      const codes = extractRedeemCodes(article.title, article.digest, text);
      for (const code of codes) {
        redeemCodes.push({
          code,
          sourceId: article.aid,
          sourceUrl: article.link,
          title: article.title,
          summary: article.digest || text.slice(0, 500),
          content: text,
          publishedAt: article.publishedAt
        });
      }
    } catch (error) {
      await updateWechatArticleDetail({
        aid: article.aid,
        html: '',
        text: '',
        fetchStatus: 'failed',
        fetchError: error instanceof Error ? error.message : '未知错误'
      });
    }
  }

  return redeemCodes;
}

export async function pollWechatRedeemCodes(): Promise<WechatRedeemCodePollResult> {
  const articles = await fetchWechatArticleList();
  const articleResult = await upsertWechatArticles(articles);
  const redeemCodes = await fetchNewArticleDetails(articleResult.insertedAids);
  const codeResult = await upsertRedeemCodes(redeemCodes);

  return {
    foundArticles: articles.length,
    insertedArticles: articleResult.insertedAids.length,
    foundCodes: redeemCodes.length,
    inserted: codeResult.inserted,
    updated: codeResult.updated,
    insertedCodes: codeResult.insertedCodes
  };
}

export function startWechatRedeemCodePolling(options?: { onNewCodes?: (codes: string[]) => void | Promise<void> }): void {
  if (pollTimer) {
    return;
  }

  const runPoll = async () => {
    if (pollingPaused || pollInFlight || disabledByInvalidSession) {
      return;
    }

    pollInFlight = true;
    try {
      const result = await pollWechatRedeemCodes();
      if (result.inserted > 0) {
        // eslint-disable-next-line no-console
        console.log(
          `WeChat redeem codes synced: articles=${result.foundArticles}, newArticles=${result.insertedArticles}, inserted=${result.inserted}, updated=${result.updated}`
        );
        await options?.onNewCodes?.(result.insertedCodes);
      }
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error('failed to poll WeChat redeem codes', error);
    } finally {
      pollInFlight = false;
    }
  };

  void runPoll();
  pollTimer = setInterval(() => {
    void runPoll();
  }, POLL_INTERVAL_MS);
}

export function pauseWechatRedeemCodePolling(): void {
  pollingPaused = true;
}

export function resumeWechatRedeemCodePolling(): void {
  pollingPaused = false;
}
