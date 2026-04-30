export interface RedeemSummary {
  total: number;
  processed: number;
  successCount: number;
  receivedCount: number;
  failureCount: number;
  remaining: number;
  resetTriggered: boolean;
}

export interface RedeemProgressPayload {
  type: 'start' | 'log' | 'progress' | 'done';
  level?: 'info' | 'warn' | 'error' | 'success';
  message?: string;
  processed?: number;
  total?: number;
  summary?: RedeemSummary;
}

export interface RedeemRunOptions {
  includeAllAccounts?: boolean;
  includeTargetAccounts?: boolean;
}

export interface ApiEnvelope {
  code?: number;
  msg?: string;
  data?: Record<string, unknown>;
}
