import { afterEach, describe, expect, it, vi } from 'vitest';
import { createWSEClient, type WSEClient } from '../core/client';

let client: WSEClient | null = null;

afterEach(() => {
  client?.destroy();
  client = null;
});

describe('the outbox of a client with no connection', () => {
  it('says a message not written within its time ran out, on its own timer', async () => {
    client = createWSEClient({ endpoints: ['ws://outbox.test/wse'] });
    const notDelivered: Array<{ reason: string; ids: string[] }> = [];
    client.on('wseNotDelivered', (detail) => notDelivered.push(detail));
    expect(client.send('order', { n: 1 }, { id: 'late', ttl: 20 })).toBe('queued');
    expect(client.send('order', { n: 2 }, { id: 'on time' })).toBe('queued');
    await vi.waitFor(() => expect(notDelivered).toEqual([{ reason: 'expired', ids: ['late'] }]), {
      timeout: 2_000,
    });
  });

  it('says what waited when the user signs out, and when the client ends', () => {
    const seen: Array<{ reason: string; ids: string[] }> = [];
    client = createWSEClient({ endpoints: ['ws://outbox.test/wse'] });
    client.on('wseNotDelivered', (detail) => seen.push(detail));
    client.send('order', { n: 1 }, { id: 'a' });
    client.signOut();
    expect(seen).toEqual([{ reason: 'cleared', ids: ['a'] }]);

    const other = createWSEClient({ endpoints: ['ws://outbox.test/wse'] });
    const closed: Array<{ reason: string; ids: string[] }> = [];
    other.on('wseNotDelivered', (detail) => closed.push(detail));
    other.send('order', { n: 1 }, { id: 'b' });
    other.destroy();
    expect(closed).toEqual([{ reason: 'closed', ids: ['b'] }]);
  });

  it('refuses a message when the outbox is full', () => {
    client = createWSEClient({ endpoints: ['ws://outbox.test/wse'], offline: { maxSize: 1 } });
    expect(client.send('order', { n: 1 })).toBe('queued');
    expect(client.send('order', { n: 2 })).toBe('dropped');
  });

  it('keeps no outbox when told not to', () => {
    client = createWSEClient({ endpoints: ['ws://outbox.test/wse'], offline: { enabled: false } });
    const seen: unknown[] = [];
    client.on('wseNotDelivered', (detail) => seen.push(detail));
    expect(client.send('order', { n: 1 })).toBe('queued');
    client.destroy();
    client = null;
    expect(seen).toEqual([]);
  });
});
