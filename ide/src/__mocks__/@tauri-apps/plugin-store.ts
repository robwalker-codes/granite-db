const stores = new Map<string, Map<string, unknown>>();

function getStore(file: string): Map<string, unknown> {
  let store = stores.get(file);
  if (!store) {
    store = new Map();
    stores.set(file, store);
  }
  return store;
}

export class Store {
  #file: string;

  constructor(file: string) {
    this.#file = file;
    getStore(file);
  }

  async get<T>(key: string): Promise<T | undefined> {
    return getStore(this.#file).get(key) as T | undefined;
  }

  async set(key: string, value: unknown): Promise<void> {
    getStore(this.#file).set(key, value);
  }

  async save(): Promise<void> {
    // no-op for tests
  }
}

export function __resetStores(): void {
  stores.clear();
}

export function __inspectStore(file: string): Map<string, unknown> {
  return getStore(file);
}
