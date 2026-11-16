import { RenderMode, ServerRoute } from '@angular/ssr';

// Rendered on the server for each request: the client must take nothing there.
export const serverRoutes: ServerRoute[] = [{ path: '**', renderMode: RenderMode.Server }];
