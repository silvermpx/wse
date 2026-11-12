import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['ts-client/**/*.test.ts', 'ts-client/**/*.test.tsx'],
    environment: 'node',
    restoreMocks: true,
    // The client logs warnings on purpose in failure scenarios; keep test output readable.
    silent: 'passed-only',
  },
});
