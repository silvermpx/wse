// @vitest-environment happy-dom
//
// An Angular application on Zone.js: the helpers take the connection outside
// Angular's zone, so the socket and its timers never keep the application
// from becoming stable (NG0506), and a change still renders.

import 'zone.js';
import '@angular/compiler';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { ApplicationRef, Component, NgZone, provideZoneChangeDetection } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { BrowserTestingModule, platformBrowserTesting } from '@angular/platform-browser/testing';
import { FakeWebSocket, connectServer, installFakeWebSocket, resetClientState, tick } from './helpers.js';
import { createWSEClient, type WSEClient } from '../core/index.js';
import { WSE_CLIENT, injectWSEState, injectWSETopics } from '../angular/index.js';

let client: WSEClient;

beforeAll(() => {
  TestBed.initTestEnvironment(BrowserTestingModule, platformBrowserTesting());
});

beforeEach(() => {
  installFakeWebSocket();
  resetClientState();
  client = createWSEClient({ endpoints: ['ws://zone.test/wse'], offline: { enabled: false } });
});

afterEach(() => {
  TestBed.resetTestingModule();
  client.destroy();
  resetClientState();
  vi.unstubAllGlobals();
});

function stableWithin(ms: number): Promise<boolean> {
  const app = TestBed.inject(ApplicationRef);
  return Promise.race([
    app.whenStable().then(() => true),
    new Promise<boolean>((done) => setTimeout(() => done(false), ms)),
  ]);
}

describe('an Angular application on Zone.js', () => {
  it('becomes stable with the connection held, and renders its changes', async () => {
    const Status = Component({ selector: 'wse-status', template: '{{ state().connection }}' })(
      class {
        state = injectWSEState();
        constructor() {
          injectWSETopics(['news']);
        }
      },
    );
    // A browser's WebSocket and timers belong to the zone they are made in:
    // the connection is taken outside Angular's.
    const zones: boolean[] = [];
    const retain = client.retain.bind(client);
    vi.spyOn(client, 'retain').mockImplementation(() => {
      zones.push(NgZone.isInAngularZone());
      return retain();
    });
    TestBed.configureTestingModule({
      providers: [provideZoneChangeDetection(), { provide: WSE_CLIENT, useValue: client }],
    });
    const fixture = TestBed.createComponent(Status);
    fixture.detectChanges();
    await tick();
    expect(zones).toEqual([false, false]);
    expect(FakeWebSocket.instances).toHaveLength(1);
    await connectServer(FakeWebSocket.last());
    expect(await stableWithin(500)).toBe(true);
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toBe('connected');
    fixture.destroy();
  });
});
