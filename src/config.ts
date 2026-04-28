import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';

interface AppConfig {
  redeemToken?: string;
  wechatMp?: {
    token?: string;
    cookie?: string;
    userAgent?: string;
    fakeid?: string;
  };
}

const DEFAULT_CONFIG: AppConfig = {};

function getConfigPath(): string {
  return path.join(process.cwd(), 'config.json');
}

function readConfig(): AppConfig {
  const configPath = getConfigPath();
  if (!fs.existsSync(configPath)) {
    return { ...DEFAULT_CONFIG };
  }

  try {
    const content = fs.readFileSync(configPath, 'utf8');
    const parsed = JSON.parse(content) as AppConfig;
    return {
      ...DEFAULT_CONFIG,
      ...parsed
    };
  } catch {
    return { ...DEFAULT_CONFIG };
  }
}

function writeConfig(config: AppConfig): void {
  const configPath = getConfigPath();
  fs.mkdirSync(path.dirname(configPath), { recursive: true });
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2), 'utf8');
}

export function getRedeemToken(): string {
  const config = readConfig();
  return config.redeemToken?.trim() || process.env.REDEEM_TOKEN?.trim() || '';
}

export function setRedeemToken(token: string): void {
  const config = readConfig();
  config.redeemToken = token.trim();
  writeConfig(config);
}

export function getRedeemConfig(): { redeemToken: string } {
  return { redeemToken: getRedeemToken() };
}

export interface WechatMpConfig {
  token: string;
  cookie: string;
  userAgent: string;
  fakeid: string;
}

export function getWechatMpConfig(): WechatMpConfig {
  const config = readConfig();
  return {
    token: config.wechatMp?.token?.trim() || process.env.WECHAT_MP_TOKEN?.trim() || '',
    cookie: config.wechatMp?.cookie?.trim() || process.env.WECHAT_MP_COOKIE?.trim() || '',
    userAgent:
      config.wechatMp?.userAgent?.trim() ||
      process.env.WECHAT_MP_USER_AGENT?.trim() ||
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    fakeid: config.wechatMp?.fakeid?.trim() || process.env.WECHAT_MP_FAKEID?.trim() || 'MzE5MTIzOTAzNQ=='
  };
}

export function setWechatMpSession(input: { token: string; cookie: string; userAgent: string; fakeid?: string }): void {
  const config = readConfig();
  config.wechatMp = {
    ...(config.wechatMp ?? {}),
    token: input.token.trim(),
    cookie: input.cookie.trim(),
    userAgent: input.userAgent.trim(),
    fakeid: input.fakeid?.trim() || config.wechatMp?.fakeid?.trim() || process.env.WECHAT_MP_FAKEID?.trim() || 'MzE5MTIzOTAzNQ=='
  };
  writeConfig(config);
}
