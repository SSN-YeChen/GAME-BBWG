import 'dotenv/config';
import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { cleanupVisitorLogs, getDb } from './core/db.js';
import { AutoRedeemCoordinator } from './services/autoRedeem.js';
import { RedeemService } from './services/redeem.js';
import {
  pauseTapTapRedeemCodePolling,
  pollTapTapRedeemCodes,
  resumeTapTapRedeemCodePolling,
  startTapTapRedeemCodePolling
} from './sources/taptapRedeemCodes.js';
import { loginWechatMpByQrCode } from './sources/wechatLogin.js';
import {
  enableWechatRedeemCodePolling,
  pauseWechatRedeemCodePolling,
  pollWechatRedeemCodes,
  resumeWechatRedeemCodePolling,
  startWechatRedeemCodePolling,
  validateWechatMpSession
} from './sources/wechatOfficial.js';
import { AuthService } from './server/auth.js';
import { createVisitorAuditMiddleware, createVisitorBlacklistMiddleware } from './server/http.js';
import { registerApiRoutes } from './server/routes.js';
import { SseHub } from './server/sse.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const redeemService = new RedeemService();
const sseHub = new SseHub();
const VISITOR_LOG_RETENTION_DAYS = 30;
const VISITOR_LOG_CLEANUP_INTERVAL_MS = 1000 * 60 * 60 * 24;
const useWechatSource = process.argv.includes('--wechat');
let wechatPollingAvailable = true;

const adminUsername = process.env.ADMIN_USERNAME?.trim() || '';
const adminPassword = process.env.ADMIN_PASSWORD?.trim() || '';
const authService = new AuthService({
  adminUsername,
  adminPassword,
  tempUsername: process.env.TEMP_USERNAME?.trim() || '',
  tempPassword: process.env.TEMP_PASSWORD?.trim() || '',
  sessionSecret: process.env.SESSION_SECRET?.trim() || 'bbwg-dev-session-secret'
});

const autoRedeemCoordinator = new AutoRedeemCoordinator({
  redeemService,
  pauseSourcePolling: pauseActiveRedeemCodePolling,
  resumeSourcePolling: resumeActiveRedeemCodePolling
});

app.set('trust proxy', true);
app.use(express.json());
app.use(createVisitorAuditMiddleware(authService));
app.use(createVisitorBlacklistMiddleware());
app.use(express.static(path.resolve(__dirname, '../public')));

redeemService.on('progress', (payload) => {
  sseHub.broadcastRedeemProgress(payload);
});

registerApiRoutes({
  app,
  authService,
  redeemService,
  autoRedeemCoordinator,
  sseHub,
  pollActiveRedeemCodeSource,
  visitorLogRetentionDays: VISITOR_LOG_RETENTION_DAYS,
  hasAdminCredentials: Boolean(adminUsername && adminPassword)
});

app.get('*', (_req, res) => {
  res.sendFile(path.resolve(__dirname, '../public/index.html'));
});

async function pollActiveRedeemCodeSource(): Promise<{ insertedCodes: string[] }> {
  return useWechatSource ? pollWechatRedeemCodes() : pollTapTapRedeemCodes();
}

function startActiveRedeemCodePolling(): void {
  if (useWechatSource) {
    if (!wechatPollingAvailable) {
      // eslint-disable-next-line no-console
      console.warn('redeem code source: WeChat official account is disabled because login failed');
      return;
    }

    // eslint-disable-next-line no-console
    console.log('redeem code source: WeChat official account');
    startWechatRedeemCodePolling({
      onNewCodes: (codes) => autoRedeemCoordinator.enqueueAutoRedeemCodes(codes)
    });
    return;
  }

  // eslint-disable-next-line no-console
  console.log('redeem code source: TapTap official topic');
  startTapTapRedeemCodePolling({
    onNewCodes: (codes) => autoRedeemCoordinator.enqueueAutoRedeemCodes(codes)
  });
}

function pauseActiveRedeemCodePolling(): void {
  if (useWechatSource) {
    pauseWechatRedeemCodePolling();
    return;
  }

  pauseTapTapRedeemCodePolling();
}

function resumeActiveRedeemCodePolling(): void {
  if (useWechatSource) {
    resumeWechatRedeemCodePolling();
    return;
  }

  resumeTapTapRedeemCodePolling();
}

async function initializeWechatSource(): Promise<void> {
  if (!useWechatSource) {
    return;
  }

  try {
    // eslint-disable-next-line no-console
    console.log('正在校验微信公众平台登录态...');
    const sessionValid = await validateWechatMpSession();
    if (sessionValid) {
      enableWechatRedeemCodePolling();
      // eslint-disable-next-line no-console
      console.log('微信公众平台登录态有效，跳过扫码登录。');
      return;
    }

    // eslint-disable-next-line no-console
    console.log('微信公众平台未登录或登录态已失效，开始扫码登录。');
    await loginWechatMpByQrCode();
    enableWechatRedeemCodePolling();
  } catch (error) {
    wechatPollingAvailable = false;
    // eslint-disable-next-line no-console
    console.error('WeChat login failed, web server will continue without WeChat polling', error);
  }
}

function startVisitorLogCleanup(): void {
  void cleanupVisitorLogs(VISITOR_LOG_RETENTION_DAYS).catch((error: unknown) => {
    // eslint-disable-next-line no-console
    console.error('failed to cleanup visitor logs on startup', error);
  });

  setInterval(() => {
    void cleanupVisitorLogs(VISITOR_LOG_RETENTION_DAYS).catch((error: unknown) => {
      // eslint-disable-next-line no-console
      console.error('failed to cleanup visitor logs', error);
    });
  }, VISITOR_LOG_CLEANUP_INTERVAL_MS);
}

const port = Number(process.env.PORT || 3458);

void getDb()
  .then(async () => {
    await initializeWechatSource();
    startActiveRedeemCodePolling();
    startVisitorLogCleanup();

    app.listen(port, () => {
      // eslint-disable-next-line no-console
      console.log(`bb-web is running at http://localhost:${port}`);
    });
  })
  .catch((error: unknown) => {
    // eslint-disable-next-line no-console
    console.error('failed to start bb-web', error);
    process.exitCode = 1;
  });
