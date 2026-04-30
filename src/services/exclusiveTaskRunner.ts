export class ExclusiveTaskRunner {
  private taskChain: Promise<void> = Promise.resolve();

  constructor(
    private readonly options: {
      isBlocked: () => boolean;
      waitMs?: number;
    }
  ) {}

  async run<T>(task: () => Promise<T>): Promise<T> {
    const previousTask = this.taskChain;
    let releaseTask: () => void = () => undefined;
    this.taskChain = new Promise<void>((resolve) => {
      releaseTask = resolve;
    });

    await previousTask;
    try {
      while (this.options.isBlocked()) {
        await sleep(this.options.waitMs ?? 5000);
      }
      return await task();
    } finally {
      releaseTask();
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}
