// The page's script, one classic script with React in it.
export default {
  define: { 'process.env.NODE_ENV': '"production"' },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    lib: { entry: 'page.ts', formats: ['iife'], name: 'check', fileName: () => 'page.js' },
  },
};
