// The application's own preload: one CommonJS script, Electron left out,
// as a sandboxed preload must be. Built after the page, beside it.
export default {
  build: {
    outDir: 'dist',
    emptyOutDir: false,
    lib: { entry: 'preload.ts', formats: ['cjs'], fileName: () => 'preload.cjs' },
    rollupOptions: { external: ['electron'] },
  },
};
