// Angular 20 as its CLI sets an application up, with a WSE client per
// injector: one per request on the server, one in the browser.
import { ApplicationConfig, provideBrowserGlobalErrorListeners, provideZoneChangeDetection } from '@angular/core';
import { provideClientHydration, withEventReplay } from '@angular/platform-browser';
import { provideRouter } from '@angular/router';
import { createWSEClient, provideWSEClient } from 'wse-client/angular';
import { routes } from './app.routes';
import { wseEndpoint } from './endpoint';

export const appConfig: ApplicationConfig = {
  providers: [
    provideBrowserGlobalErrorListeners(),
    provideZoneChangeDetection({ eventCoalescing: true }),
    provideRouter(routes),
    provideClientHydration(withEventReplay()),
    provideWSEClient(() => createWSEClient({ endpoints: [wseEndpoint()] })),
  ],
};
