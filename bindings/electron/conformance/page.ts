// The conformance cases in a page of a real Electron window: the case's
// client is the page's (createElectronClient), over the port the preload
// hands it; the owner, in the main process, holds the connection as the
// case asks for it (main.js).

import { createElectronClient } from '../../../ts-client/electron/renderer.js';
import { runCases, type Asked, type Made } from '../../../ts-client/__tests__/conformance/driver.js';
import { logger, LogLevel } from '../../../ts-client/utils/logger.js';

const given = new URLSearchParams(location.hash.slice(1));

/** Asks the main process for an owner as the case wants it, then makes the page's client. */
async function make(asked: Asked): Promise<Made> {
  const { application, ...connection } = asked;
  const answer = await (await fetch('app://harness/owner', { method: 'POST', body: JSON.stringify(connection) })).json();
  if (answer.error) throw new Error(`the main process: ${answer.error}`);
  return { client: createElectronClient(application), publicKey: answer.publicKey ?? null };
}

function report(failed: string[]): void {
  void fetch('app://harness/done', { method: 'POST', body: JSON.stringify({ failed }) });
}

logger.setLevel(LogLevel.ERROR);
runCases({
  control: given.get('control') ?? '',
  agent: 'electron',
  runtime: given.get('runtime') ?? 'electron',
  os: given.get('os') ?? '',
  arch: given.get('arch') ?? '',
  make,
  bridged: true,
  only: given.get('cases')?.split(','),
}).then(report, (error) => report([`the driver: ${error instanceof Error ? error.message : String(error)}`]));
