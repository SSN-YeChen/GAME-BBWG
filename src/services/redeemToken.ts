import vm from 'node:vm';

const GIFT_CODE_SITE_URL = 'https://giftcode.benbenwangguo.cn/';

function extractScriptUrlsFromHtml(html: string, baseUrl: string): string[] {
  const scriptUrls = new Set<string>();
  const scriptTagPattern = /<script\b[^>]*\bsrc=(['"])([^'"]+)\1[^>]*>/gi;

  for (const match of html.matchAll(scriptTagPattern)) {
    const rawUrl = match[2]?.trim();
    if (!rawUrl) {
      continue;
    }

    try {
      const resolvedUrl = new URL(rawUrl, baseUrl);
      if (resolvedUrl.pathname.endsWith('.js')) {
        scriptUrls.add(resolvedUrl.toString());
      }
    } catch {
      // ignore invalid script urls
    }
  }

  return Array.from(scriptUrls);
}

function extractLazyChunkUrlsFromScript(scriptUrl: string, scriptContent: string): string[] {
  const chunkUrls = new Set<string>();
  const chunkNamePattern = /src_pages_[A-Za-z0-9_]+_vue/g;
  const hashMatch = new URL(scriptUrl).pathname.match(/\.([a-f0-9]{8,})\.js$/i);
  const chunkHash = hashMatch?.[1];

  if (!chunkHash) {
    return [];
  }

  for (const match of scriptContent.matchAll(chunkNamePattern)) {
    const chunkName = match[0]?.trim();
    if (!chunkName) {
      continue;
    }

    const chunkUrl = new URL(`/js/${chunkName}.${chunkHash}.js`, scriptUrl);
    chunkUrls.add(chunkUrl.toString());
  }

  return Array.from(chunkUrls);
}

function getVmDecoderCandidates(sandbox: Record<string, unknown>): Array<(value: number) => unknown> {
  const decoderCandidates: Array<(value: number) => unknown> = [];
  const visited = new Set<unknown>();
  const queue: unknown[] = [sandbox, sandbox.self];

  while (queue.length > 0) {
    const current = queue.shift();
    if (!current || typeof current !== 'object' || visited.has(current)) {
      continue;
    }

    visited.add(current);

    for (const value of Object.values(current)) {
      if (!value) {
        continue;
      }

      if (typeof value === 'function') {
        decoderCandidates.push(value as (value: number) => unknown);
        continue;
      }

      if (typeof value === 'object' && !visited.has(value)) {
        queue.push(value);
      }
    }
  }

  return decoderCandidates;
}

function decodeSaltFromModuleSource(
  moduleSource: string,
  decoderCandidates: Array<(value: number) => unknown>
): string | null {
  const directSaltPattern = /MD5\([\s\S]*?\+\s*['"`]([A-Za-z0-9]{16,})['"`]\s*\)/;
  const directSaltMatch = moduleSource.match(directSaltPattern);
  if (directSaltMatch?.[1]) {
    return directSaltMatch[1];
  }

  const md5CallMatch = moduleSource.match(/MD5\([\s\S]*?\+\s*([_$a-zA-Z0-9]+)\((0x[0-9a-f]+)\)[\s\S]*?\)/i);
  if (!md5CallMatch?.[2]) {
    return null;
  }

  const indexValue = Number.parseInt(md5CallMatch[2], 16);
  if (!Number.isFinite(indexValue)) {
    return null;
  }

  for (const decoder of decoderCandidates) {
    try {
      const decodedValue = decoder(indexValue);
      if (typeof decodedValue === 'string' && /^[A-Za-z0-9]{16,}$/.test(decodedValue)) {
        return decodedValue;
      }
    } catch {
      // ignore decoder failures
    }
  }

  return null;
}

function extractRedeemSaltFromBundle(bundleContent: string): string | null {
  const sandbox: Record<string, unknown> = {
    self: {},
    window: {},
    globalThis: {},
    console: {
      log: () => undefined,
      error: () => undefined,
      warn: () => undefined
    }
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;

  try {
    vm.runInNewContext(bundleContent, sandbox, {
      timeout: 1_500
    });
  } catch {
    return null;
  }

  const decoderCandidates = getVmDecoderCandidates(sandbox);
  const chunkArrays = Object.values(sandbox.self as Record<string, unknown>).filter(Array.isArray) as unknown[][];

  for (const chunkArray of chunkArrays) {
    for (const chunkEntry of chunkArray) {
      if (!Array.isArray(chunkEntry) || chunkEntry.length < 2) {
        continue;
      }

      const modules = chunkEntry[1];
      if (!modules || typeof modules !== 'object') {
        continue;
      }

      for (const moduleFactory of Object.values(modules as Record<string, unknown>)) {
        if (typeof moduleFactory !== 'function') {
          continue;
        }

        const moduleSource = moduleFactory.toString();
        if (!moduleSource.includes('appendSign') && !moduleSource.includes('MD5')) {
          continue;
        }

        const salt = decodeSaltFromModuleSource(moduleSource, decoderCandidates);
        if (salt) {
          return salt;
        }
      }
    }
  }

  return null;
}

async function fetchText(url: string): Promise<string> {
  const response = await fetch(url, {
    headers: {
      'User-Agent': 'bb-web/1.0'
    }
  });

  if (!response.ok) {
    throw new Error(`请求失败: HTTP ${response.status} (${url})`);
  }

  return response.text();
}

export async function fetchRemoteRedeemToken(): Promise<{ token: string; sourceUrl: string }> {
  const html = await fetchText(GIFT_CODE_SITE_URL);
  const initialScriptUrls = extractScriptUrlsFromHtml(html, GIFT_CODE_SITE_URL);
  const scriptUrlSet = new Set(initialScriptUrls);

  for (const scriptUrl of initialScriptUrls) {
    if (!scriptUrl.includes('/app.')) {
      continue;
    }

    try {
      const appScriptContent = await fetchText(scriptUrl);
      const lazyChunkUrls = extractLazyChunkUrlsFromScript(scriptUrl, appScriptContent);
      for (const lazyChunkUrl of lazyChunkUrls) {
        scriptUrlSet.add(lazyChunkUrl);
      }
    } catch {
      // keep using the directly discovered script urls
    }
  }

  const scriptUrls = Array.from(scriptUrlSet).sort((left, right) => {
    const leftPriority = left.includes('src_pages_home_index_vue')
      ? 0
      : left.includes('/app.')
        ? 1
        : 2;
    const rightPriority = right.includes('src_pages_home_index_vue')
      ? 0
      : right.includes('/app.')
        ? 1
        : 2;
    return leftPriority - rightPriority;
  });

  for (const scriptUrl of scriptUrls) {
    try {
      const scriptContent = await fetchText(scriptUrl);
      const token = extractRedeemSaltFromBundle(scriptContent);
      if (token) {
        return {
          token,
          sourceUrl: scriptUrl
        };
      }
    } catch {
      // try next script
    }
  }

  throw new Error('未能从目标站点的 JS 文件中提取到 TOKEN。');
}
