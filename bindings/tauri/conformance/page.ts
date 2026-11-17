// The conformance cases in a page of a real Tauri webview: the case's
// client is the page's (createTauriClient), through the plugin's commands
// and a Channel; the owner, on the Rust side, holds the connection as the
// case asks for it (src/main.rs).

import { createTauriClient, type TauriCore } from '../../../ts-client/tauri/index.js';
import { runCases, type Asked, type Made } from '../../../ts-client/__tests__/conformance/driver.js';
import { logger, LogLevel } from '../../../ts-client/utils/logger.js';

interface Given {
  control: string;
  cases: string | null;
  os: string;
  arch: string;
  runtime: string;
}

const core = (window as unknown as { __TAURI__: { core: TauriCore } }).__TAURI__.core;

/** Asks the Rust side for an owner as the case wants it, then makes the page's client. */
async function make(asked: Asked): Promise<Made> {
  const { application, ...connection } = asked;
  const answer = await core.invoke<{ publicKey: string | null }>('harness_owner', { asked: connection });
  return { client: createTauriClient(core, application), publicKey: answer.publicKey };
}

async function main(): Promise<void> {
  const given = await core.invoke<Given>('harness_given');
  let failed: string[];
  try {
    failed = await runCases({
      control: given.control,
      agent: 'tauri',
      runtime: given.runtime,
      os: given.os,
      arch: given.arch,
      make,
      bridged: true,
      only: given.cases?.split(','),
    });
  } catch (error) {
    failed = [`the driver: ${error instanceof Error ? error.message : String(error)}`];
  }
  await core.invoke('harness_done', { failed });
}

logger.setLevel(LogLevel.ERROR);
void main();
