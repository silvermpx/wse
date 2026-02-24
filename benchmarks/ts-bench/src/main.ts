import { parseConfig } from './config.js';
import { checkPreflight } from './preflight.js';
import { generateBenchToken } from './jwt.js';
import { connectAndHandshake, parseWseMessage } from './protocol.js';
import { runTests } from './tests/index.js';
import { toJson, toMarkdown } from './report.js';

async function main() {
  const config = parseConfig();

  console.log('WSE Benchmark Suite (TypeScript)');
  console.log('================================');
  console.log(`  Server:  ${config.host}:${config.port}`);
  console.log(`  Secret:  ${config.secret.slice(0, 8)}...`);
  console.log(`  Duration: ${config.duration}s per tier`);
  if (config.tiers) {
    console.log(`  Custom tiers: [${config.tiers.join(', ')}]`);
  }
  console.log();

  // Preflight
  const maxConns = config.tiers
    ? Math.max(...config.tiers)
    : config.maxConnections;
  checkPreflight(maxConns);

  // Connectivity check
  console.log();
  process.stdout.write('  Connectivity check: ');
  try {
    const token = generateBenchToken(Buffer.from(config.secret), 'bench-user');
    const ws = await connectAndHandshake(
      config.host, config.port, token,
      'compression=false&protocol_version=1', 10
    );
    ws.close();
    console.log('OK (connected, server_ready received)');
  } catch (err) {
    console.log(`FAILED: ${err}`);
    process.exit(1);
  }

  // Run tests
  const reports = await runTests(config);

  // Output
  if (config.output === 'json') {
    console.log(toJson(reports));
  } else if (config.output === 'markdown') {
    console.log(toMarkdown(reports));
  }

  console.log('\nDone.');
  process.exit(0);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
