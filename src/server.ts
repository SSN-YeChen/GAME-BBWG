import 'dotenv/config';
import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { getDb } from './core/db.js';
import { ActiveRedeemCodeSource } from './sources/activeRedeemCodeSource.js';
import { AccountImportService } from './services/accountImport.js';
import { AutoRedeemCoordinator } from './services/autoRedeem.js';
import { RedeemService } from './services/redeem.js';
import { AuthService } from './server/auth.js';
import { createVisitorAuditMiddleware, createVisitorBlacklistMiddleware } from './server/http.js';
import { startVisitorLogCleanup, VISITOR_LOG_RETENTION_DAYS } from './server/maintenance.js';
import { registerApiRoutes } from './server/routes.js';
import { SseHub } from './server/sse.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const redeemService = new RedeemService();
const accountImportService = new AccountImportService();
const sseHub = new SseHub();
const redeemCodeSource = new ActiveRedeemCodeSource(process.argv.includes('--wechat'));

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
  pauseSourcePolling: () => redeemCodeSource.pause(),
  resumeSourcePolling: () => redeemCodeSource.resume()
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
  accountImportService,
  autoRedeemCoordinator,
  sseHub,
  pollActiveRedeemCodeSource: () => redeemCodeSource.poll(),
  visitorLogRetentionDays: VISITOR_LOG_RETENTION_DAYS,
  hasAdminCredentials: Boolean(adminUsername && adminPassword)
});

app.get('*', (_req, res) => {
  res.sendFile(path.resolve(__dirname, '../public/index.html'));
});

const port = Number(process.env.PORT || 3458);

void getDb()
  .then(async () => {
    await redeemCodeSource.initialize();
    redeemCodeSource.start(autoRedeemCoordinator);
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
