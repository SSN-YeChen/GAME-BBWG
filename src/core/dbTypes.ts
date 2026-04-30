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
