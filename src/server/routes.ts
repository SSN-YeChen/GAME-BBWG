import { registerAccountRoutes } from './routes/accounts.js';
import { registerAuthRoutes } from './routes/auth.js';
import { registerConfigRoutes } from './routes/config.js';
import { registerRedeemRoutes } from './routes/redeem.js';
import type { ApiRouteContext } from './routes/types.js';
import { registerVisitorRoutes } from './routes/visitor.js';

export function registerApiRoutes(context: ApiRouteContext): void {
  registerAuthRoutes(context);
  registerRedeemRoutes(context);
  registerAccountRoutes(context);
  registerConfigRoutes(context);
  registerVisitorRoutes(context);
}
