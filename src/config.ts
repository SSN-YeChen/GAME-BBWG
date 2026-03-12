import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';

interface AppConfig {
  redeemToken?: string;
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
