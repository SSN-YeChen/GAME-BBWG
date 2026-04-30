export function normalizeWhitespace(value: string): string {
  return value.replace(/\s+/g, ' ').trim();
}

export function extractRedeemCodes(...texts: string[]): string[] {
  const codes = new Set<string>();
  const sourceText = texts.filter(Boolean).join('\n');
  const explicitCodePattern = /(?:兑换码|礼包码|CDK|cdk)\s*[：:：\s]\s*([A-Za-z0-9][A-Za-z0-9_-]{2,31})/gu;

  for (const match of sourceText.matchAll(explicitCodePattern)) {
    const code = match[1]?.replace(/[^A-Za-z0-9_-]/g, '').trim();
    if (code) {
      codes.add(code.toUpperCase());
    }
  }

  return Array.from(codes);
}
