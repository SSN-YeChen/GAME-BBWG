import { sendJsonError } from '../http.js';
import type { ApiRouteContext } from './types.js';

export function registerAuthRoutes({ app, authService, hasAdminCredentials }: ApiRouteContext): void {
  app.get('/api/auth/status', (req, res) => {
    const session = authService.getSessionFromRequest(req);
    res.json({
      authenticated: Boolean(session),
      username: session?.username ?? '',
      role: session?.role ?? ''
    });
  });

  app.post('/api/auth/login', (req, res) => {
    const { username, password } = req.body as { username?: string; password?: string };
    const normalizedUsername = username?.trim() ?? '';
    const normalizedPassword = password?.trim() ?? '';

    if (!hasAdminCredentials) {
      sendJsonError(res, new Error('服务端未配置登录账号或密码。请先设置 .env。'), 500);
      return;
    }

    const matchedRole = authService.verifyCredentials(normalizedUsername, normalizedPassword);
    if (!matchedRole) {
      sendJsonError(res, new Error('账号或密码错误。'), 401);
      return;
    }

    const token = authService.createSession(normalizedUsername, matchedRole);
    authService.setSessionCookie(res, token);

    res.json({
      ok: true,
      username: normalizedUsername,
      role: matchedRole
    });
  });

  app.post('/api/auth/logout', (req, res) => {
    authService.clearSessionFromRequest(req);
    authService.clearSessionCookie(res);
    res.json({ ok: true });
  });
}
