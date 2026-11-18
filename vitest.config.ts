import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

// The Solid adapter's tests run on the builds Solid ships for a browser in
// development, once on Solid 1.9 and once on 2.0 (installed as solid-js-next).
const solid = (major: string, alias: { find: RegExp; replacement: string }[]) => ({
  extends: true as const,
  resolve: { conditions: ['browser', 'development'], alias },
  test: {
    name: `solid ${major}`,
    include: ['ts-client/__tests__/solid/**/*.test.ts'],
    exclude: ['**/*.server.test.ts'],
    environment: 'happy-dom',
    env: { SOLID_MAJOR: major },
    server: { deps: { inline: [/solid-js/] } },
  },
});

export default defineConfig({
  test: {
    restoreMocks: true,
    // The client logs warnings on purpose in failure scenarios; keep test output readable.
    silent: 'passed-only',
    projects: [
      {
        extends: true,
        // The examples import the package by its name.
        resolve: {
          alias: [
            { find: /^wse-client\/react$/, replacement: fileURLToPath(new URL('./ts-client/react/index.ts', import.meta.url)) },
          ],
        },
        test: {
          name: 'client',
          include: ['ts-client/**/*.test.ts', 'ts-client/**/*.test.tsx'],
          exclude: ['**/node_modules/**', 'ts-client/__tests__/solid/**'],
          environment: 'node',
        },
      },
      solid('1.9', []),
      solid('2.0', [{ find: /^solid-js$/, replacement: 'solid-js-next' }]),
      {
        // Solid's build for a server, as Node resolves it.
        extends: true,
        test: { name: 'solid 1.9 server', include: ['ts-client/__tests__/solid/**/*.server.test.ts'], environment: 'node' },
      },
    ],
  },
});
