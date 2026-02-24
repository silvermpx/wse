import { execFileSync } from 'node:child_process';

export function checkPreflight(maxConns: number): void {
  console.log('  Preflight checks:');

  // Check ulimit
  try {
    const raw = execFileSync('bash', ['-c', 'ulimit -n'], { encoding: 'utf8' }).trim();
    const ulimit = raw === 'unlimited' ? 999999 : parseInt(raw, 10);
    const needed = Math.floor(maxConns * 1.5) + 1000;
    if (isNaN(ulimit)) {
      console.log(`    [SKIP] ulimit -n = ${raw} (could not parse)`);
    } else if (ulimit >= needed) {
      console.log(`    [OK] ulimit -n = ${ulimit} (need ~${needed})`);
    } else {
      console.log(`    [WARN] ulimit -n = ${ulimit} (need ~${needed}). Run: ulimit -n ${needed}`);
    }
  } catch {
    console.log('    [SKIP] Could not check ulimit');
  }

  // Check platform-specific settings
  if (process.platform === 'linux') {
    checkSysctl('net.core.somaxconn', 65535);
    checkSysctl('net.ipv4.tcp_tw_reuse', 1);
  }
}

function checkSysctl(name: string, expected: number): void {
  try {
    const val = parseInt(
      execFileSync('sysctl', ['-n', name], { encoding: 'utf8' }).trim(),
      10
    );
    if (val >= expected) {
      console.log(`    [OK] ${name} = ${val}`);
    } else {
      console.log(`    [WARN] ${name} = ${val} (recommend >= ${expected})`);
    }
  } catch {
    console.log(`    [SKIP] Could not check ${name}`);
  }
}
