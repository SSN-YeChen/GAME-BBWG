export class UniqueStringQueue {
  private readonly items: string[] = [];
  private readonly queuedItems = new Set<string>();

  enqueue(value: string): boolean {
    const normalizedValue = value.trim();
    if (!normalizedValue || this.queuedItems.has(normalizedValue)) {
      return false;
    }

    this.queuedItems.add(normalizedValue);
    this.items.push(normalizedValue);
    return true;
  }

  dequeue(): string | undefined {
    return this.items.shift();
  }

  drainAll(): string[] {
    return this.items.splice(0, this.items.length);
  }

  release(value: string): void {
    this.queuedItems.delete(value);
  }

  get length(): number {
    return this.items.length;
  }
}
