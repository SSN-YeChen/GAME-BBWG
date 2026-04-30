import type express from 'express';
import type { AccountImportProgressPayload } from '../services/accountImport.js';
import type { RedeemProgressPayload } from '../services/redeem.js';

export class SseHub {
  private readonly redeemClients = new Set<express.Response>();
  private readonly importClients = new Set<express.Response>();

  broadcastRedeemProgress(payload: RedeemProgressPayload): void {
    this.broadcast(this.redeemClients, payload);
  }

  broadcastImportProgress(payload: AccountImportProgressPayload): void {
    this.broadcast(this.importClients, payload);
  }

  handleRedeemEvents(_req: express.Request, res: express.Response): void {
    this.setupStream(res);
    res.write(`data: ${JSON.stringify({ type: 'log', level: 'info', message: '已连接进度通道。' })}\n\n`);
    this.trackClient(this.redeemClients, res);
  }

  handleImportEvents(_req: express.Request, res: express.Response): void {
    this.setupStream(res);
    this.trackClient(this.importClients, res);
  }

  private broadcast(clients: Set<express.Response>, payload: unknown): void {
    const body = `data: ${JSON.stringify(payload)}\n\n`;
    for (const client of clients) {
      client.write(body);
    }
  }

  private setupStream(res: express.Response): void {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();
  }

  private trackClient(clients: Set<express.Response>, res: express.Response): void {
    clients.add(res);

    const heartbeat = setInterval(() => {
      res.write(': ping\n\n');
    }, 15_000);

    res.on('close', () => {
      clearInterval(heartbeat);
      clients.delete(res);
    });
  }
}
