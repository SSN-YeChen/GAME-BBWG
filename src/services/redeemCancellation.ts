export class RedeemCancelledError extends Error {
  constructor() {
    super('兑换已手动停止。');
    this.name = 'RedeemCancelledError';
  }
}
