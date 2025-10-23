export type DomainEvent =
  | { type: "DB_OPENED"; path: string }
  | { type: "TX_COMMIT"; db: string }
  | { type: "DDL_CHANGED"; db: string };

type Handler<TType extends DomainEvent["type"]> = (event: Extract<DomainEvent, { type: TType }>) => void;

const handlers = new Map<DomainEvent["type"], Set<Handler<any>>>();

export function publish(event: DomainEvent): void {
  const listeners = handlers.get(event.type);
  if (!listeners) {
    return;
  }
  for (const listener of listeners) {
    try {
      listener(event as never);
    } catch (error) {
      console.error(`[DomainEvents] handler for ${event.type} failed`, error);
    }
  }
}

export function subscribe<TType extends DomainEvent["type"]>(type: TType, handler: Handler<TType>): () => void {
  let listeners = handlers.get(type);
  if (!listeners) {
    listeners = new Set();
    handlers.set(type, listeners);
  }
  listeners.add(handler as Handler<any>);
  return () => {
    listeners?.delete(handler as Handler<any>);
  };
}

