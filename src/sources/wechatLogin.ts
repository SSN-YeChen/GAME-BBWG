import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import jpeg from 'jpeg-js';
import { PNG } from 'pngjs';
import qrcode from 'qrcode-terminal';
import { getWechatMpConfig, setWechatMpSession } from '../core/config.js';

const require = createRequire(import.meta.url);
const jsQR = require('jsqr') as (
  data: Uint8ClampedArray,
  width: number,
  height: number,
  options?: { inversionAttempts?: 'dontInvert' | 'onlyInvert' | 'attemptBoth' | 'invertFirst' }
) => { data: string } | null;

const MP_BASE_URL = 'https://mp.weixin.qq.com';
const LOGIN_USER_AGENT =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';
const LOGIN_TIMEOUT_MS = 180_000;

interface JsonResponse {
  base_resp?: {
    ret?: number;
    err_msg?: string;
  };
  status?: number;
  acct_size?: number;
  binduin?: string;
  redirect_url?: string;
}

interface DecodedImage {
  width: number;
  height: number;
  data: Uint8Array | Buffer;
}

class CookieJar {
  private readonly cookies = new Map<string, string>();

  addFromSetCookieHeaders(headers: Headers): void {
    const getSetCookie = (headers as unknown as { getSetCookie?: () => string[] }).getSetCookie;
    const setCookieHeaders = typeof getSetCookie === 'function' ? getSetCookie.call(headers) : [];
    const fallback = headers.get('set-cookie');

    if (setCookieHeaders.length === 0 && fallback) {
      setCookieHeaders.push(...fallback.split(/,(?=\s*[^;,=\s]+=[^;,]+)/g));
    }

    for (const setCookieHeader of setCookieHeaders) {
      const [pair] = setCookieHeader.split(';');
      const separatorIndex = pair.indexOf('=');
      if (separatorIndex === -1) {
        continue;
      }

      const key = pair.slice(0, separatorIndex).trim();
      const value = pair.slice(separatorIndex + 1).trim();
      if (key) {
        this.cookies.set(key, value);
      }
    }
  }

  toHeader(): string {
    return Array.from(this.cookies.entries())
      .map(([key, value]) => `${key}=${value}`)
      .join('; ');
  }
}

function buildHeaders(jar: CookieJar, extra?: Record<string, string>): Record<string, string> {
  const headers: Record<string, string> = {
    'User-Agent': LOGIN_USER_AGENT,
    Referer: `${MP_BASE_URL}/`,
    ...extra
  };
  const cookieHeader = jar.toHeader();
  if (cookieHeader) {
    headers.Cookie = cookieHeader;
  }
  return headers;
}

async function getJson(url: string, jar: CookieJar): Promise<JsonResponse> {
  const response = await fetch(url, {
    headers: buildHeaders(jar, {
      Accept: 'application/json, text/plain, */*',
      'X-Requested-With': 'XMLHttpRequest'
    })
  });
  jar.addFromSetCookieHeaders(response.headers);

  if (!response.ok) {
    throw new Error(`微信登录请求失败: HTTP ${response.status} (${url})`);
  }

  return (await response.json()) as JsonResponse;
}

async function postFormJson(url: string, jar: CookieJar, data: Record<string, string>): Promise<JsonResponse> {
  const response = await fetch(url, {
    method: 'POST',
    headers: buildHeaders(jar, {
      Accept: 'application/json, text/plain, */*',
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'X-Requested-With': 'XMLHttpRequest'
    }),
    body: new URLSearchParams(data)
  });
  jar.addFromSetCookieHeaders(response.headers);

  if (!response.ok) {
    throw new Error(`微信登录请求失败: HTTP ${response.status} (${url})`);
  }

  return (await response.json()) as JsonResponse;
}

async function getBuffer(url: string, jar: CookieJar): Promise<Buffer> {
  const response = await fetch(url, {
    headers: buildHeaders(jar, {
      Accept: 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8'
    })
  });
  jar.addFromSetCookieHeaders(response.headers);

  if (!response.ok) {
    throw new Error(`微信二维码请求失败: HTTP ${response.status}`);
  }

  return Buffer.from(await response.arrayBuffer());
}

function decodeQrImage(buffer: Buffer): DecodedImage {
  if (isPngBuffer(buffer)) {
    return PNG.sync.read(trimPngBuffer(buffer));
  }

  if (isJpegBuffer(buffer)) {
    return jpeg.decode(buffer, { useTArray: true });
  }

  throw new Error('二维码图片不是 PNG/JPG 格式，跳过终端渲染。');
}

function getQrContentFromImage(buffer: Buffer): string {
  const image = decodeQrImage(buffer);
  const code = jsQR(new Uint8ClampedArray(image.data), image.width, image.height);
  if (!code?.data) {
    throw new Error('未能识别二维码内容。');
  }

  return code.data;
}

function renderQrCodeToTerminal(content: string): void {
  qrcode.generate(content, { small: true });
}

function isPngBuffer(buffer: Buffer): boolean {
  return buffer.length >= 8 && buffer.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]));
}

function isJpegBuffer(buffer: Buffer): boolean {
  return buffer.length >= 3 && buffer[0] === 0xff && buffer[1] === 0xd8 && buffer[2] === 0xff;
}

function trimPngBuffer(buffer: Buffer): Buffer {
  if (!isPngBuffer(buffer)) {
    return buffer;
  }

  const iendSignature = Buffer.from([0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82]);
  const iendIndex = buffer.indexOf(iendSignature);
  if (iendIndex === -1) {
    return buffer;
  }
  return buffer.subarray(0, iendIndex + iendSignature.length);
}

function saveQrImage(buffer: Buffer): string {
  const dir = path.join(process.cwd(), 'data');
  fs.mkdirSync(dir, { recursive: true });
  const extension = isJpegBuffer(buffer) ? 'jpg' : 'png';
  const filePath = path.join(dir, `wechat-login-qrcode.${extension}`);
  fs.writeFileSync(filePath, trimPngBuffer(buffer));
  return filePath;
}

function getTokenFromRedirectUrl(redirectUrl: string): string {
  const matched = redirectUrl.match(/[?&]token=(\d+)/);
  return matched?.[1] ?? '';
}

async function initializeWechatLoginSession(jar: CookieJar): Promise<void> {
  const response = await fetch(MP_BASE_URL, {
    headers: {
      'User-Agent': LOGIN_USER_AGENT
    }
  });
  jar.addFromSetCookieHeaders(response.headers);
}

export async function loginWechatMpByQrCode(): Promise<void> {
  const config = getWechatMpConfig();
  const jar = new CookieJar();
  const sessionId = `${Date.now()}${Math.floor(Math.random() * 100)}`;

  await initializeWechatLoginSession(jar);

  const startLogin = await postFormJson(`${MP_BASE_URL}/cgi-bin/bizlogin?action=startlogin`, jar, {
    userlang: 'zh_CN',
    redirect_url: '',
    login_type: '3',
    sessionid: sessionId
  });

  if (startLogin.base_resp?.ret !== 0) {
    throw new Error(`微信扫码登录初始化失败: ${startLogin.base_resp?.err_msg ?? startLogin.base_resp?.ret ?? '未知错误'}`);
  }

  const qrUrl = `${MP_BASE_URL}/cgi-bin/scanloginqrcode?action=getqrcode&random=${Date.now()}&login_appid=`;
  const qrBuffer = await getBuffer(qrUrl, jar);
  const qrFilePath = saveQrImage(qrBuffer);
  // eslint-disable-next-line no-console
  console.log('\n请使用微信扫描下方二维码登录公众平台：\n');
  try {
    renderQrCodeToTerminal(getQrContentFromImage(qrBuffer));
  } catch (error) {
    // eslint-disable-next-line no-console
    console.warn('终端二维码渲染失败，请打开本地二维码图片扫码。', error);
  }
  // eslint-disable-next-line no-console
  console.log(`\n二维码图片已保存：${qrFilePath}\n扫码后请在手机微信中确认登录。\n`);

  const startedAt = Date.now();
  let scannedLogged = false;

  while (Date.now() - startedAt < LOGIN_TIMEOUT_MS) {
    await new Promise((resolve) => {
      setTimeout(resolve, 2000);
    });

    const askResult = await getJson(`${MP_BASE_URL}/cgi-bin/scanloginqrcode?action=ask`, jar);
    if (askResult.base_resp?.ret !== 0) {
      throw new Error(`微信扫码状态检查失败: ${askResult.base_resp?.err_msg ?? askResult.base_resp?.ret ?? '未知错误'}`);
    }

    if ((askResult.status === 4 || askResult.status === 6) && !scannedLogged) {
      scannedLogged = true;
      // eslint-disable-next-line no-console
      console.log('已扫码，等待手机确认...');
      continue;
    }

    if (askResult.status === 1) {
      const loginResult = await postFormJson(`${MP_BASE_URL}/cgi-bin/bizlogin?action=login`, jar, {
        userlang: 'zh_CN',
        redirect_url: '',
        cookie_forbidden: '0',
        cookie_cleaned: '0',
        plugin_used: '0',
        login_type: '3'
      });

      const redirectUrl = loginResult.redirect_url ?? '';
      const token = getTokenFromRedirectUrl(redirectUrl);
      if (!token) {
        throw new Error('微信扫码登录成功但未获取到 token。');
      }

      if (redirectUrl) {
        const homeUrl = redirectUrl.startsWith('http') ? redirectUrl : `${MP_BASE_URL}${redirectUrl}`;
        const homeResponse = await fetch(homeUrl, {
          headers: buildHeaders(jar)
        });
        jar.addFromSetCookieHeaders(homeResponse.headers);
      }

      setWechatMpSession({
        token,
        cookie: jar.toHeader(),
        userAgent: LOGIN_USER_AGENT,
        fakeid: config.fakeid
      });
      // eslint-disable-next-line no-console
      console.log('微信公众平台登录成功，登录态已保存到 config.json。');
      return;
    }

    if (askResult.status === 2 || askResult.status === 3) {
      throw new Error('微信扫码二维码已失效，请重启服务重新扫码。');
    }

    if (askResult.status === 5) {
      throw new Error('当前微信没有可登录的公众平台账号。');
    }
  }

  throw new Error('微信扫码登录超时，请重启服务重新扫码。');
}
