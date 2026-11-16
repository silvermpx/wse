import {
  AngularNodeAppEngine,
  createNodeRequestHandler,
  isMainModule,
  writeResponseToNodeResponse,
} from '@angular/ssr/node';
import express from 'express';
import type { AddressInfo } from 'node:net';
import { join } from 'node:path';

const browserDistFolder = join(import.meta.dirname, '../browser');

const app = express();
const angularApp = new AngularNodeAppEngine();

app.use(express.static(browserDistFolder, { maxAge: '1y', index: false, redirect: false }));

app.use((req, res, next) => {
  angularApp
    .handle(req)
    .then((response) => (response ? writeResponseToNodeResponse(response, res) : next()))
    .catch(next);
});

// PORT=0 takes a free port; the line below names the one taken.
if (isMainModule(import.meta.url)) {
  const server = app.listen(Number(process.env['PORT'] ?? 4000), (error) => {
    if (error) throw error;
    console.log(`listening on http://localhost:${(server.address() as AddressInfo).port}`);
  });
}

export const reqHandler = createNodeRequestHandler(app);
