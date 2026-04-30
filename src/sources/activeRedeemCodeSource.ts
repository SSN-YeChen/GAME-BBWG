import type { AutoRedeemCoordinator } from '../services/autoRedeem.js';
import {
  pauseTapTapRedeemCodePolling,
  pollTapTapRedeemCodes,
  resumeTapTapRedeemCodePolling,
  startTapTapRedeemCodePolling
} from './taptapRedeemCodes.js';
import { loginWechatMpByQrCode } from './wechatLogin.js';
import {
  enableWechatRedeemCodePolling,
  pauseWechatRedeemCodePolling,
  pollWechatRedeemCodes,
  resumeWechatRedeemCodePolling,
  startWechatRedeemCodePolling,
  validateWechatMpSession
} from './wechatOfficial.js';

export interface RedeemCodeSourcePollResult {
  insertedCodes: string[];
}

export class ActiveRedeemCodeSource {
  private wechatPollingAvailable = true;

  constructor(private readonly useWechatSource: boolean) {}

  async poll(): Promise<RedeemCodeSourcePollResult> {
    return this.useWechatSource ? pollWechatRedeemCodes() : pollTapTapRedeemCodes();
  }

  async initialize(): Promise<void> {
    if (!this.useWechatSource) {
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
      this.wechatPollingAvailable = false;
      // eslint-disable-next-line no-console
      console.error('WeChat login failed, web server will continue without WeChat polling', error);
    }
  }

  start(autoRedeemCoordinator: AutoRedeemCoordinator): void {
    if (this.useWechatSource) {
      if (!this.wechatPollingAvailable) {
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

  pause(): void {
    if (this.useWechatSource) {
      pauseWechatRedeemCodePolling();
      return;
    }

    pauseTapTapRedeemCodePolling();
  }

  resume(): void {
    if (this.useWechatSource) {
      resumeWechatRedeemCodePolling();
      return;
    }

    resumeTapTapRedeemCodePolling();
  }
}
