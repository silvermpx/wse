// Angular 22 as its CLI sets an application up, with a WSE client per
// injector: one per request on the server, one in the browser.
import { ApplicationConfig, provideBrowserGlobalErrorListeners } from '@angular/core';
import { provideClientHydration } from '@angular/platform-browser';
import { provideRouter } from '@angular/router';
import { createWSEClient, provideWSEClient } from 'wse-client/angular';
import { routes } from './app.routes';
import { wseEndpoint } from './endpoint';

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    provideRouter(routes),
    provideClientHydration(),
    provideWSEClient(() => createWSEClient({ endpoints: [wseEndpoint()] })),
  ],
};
