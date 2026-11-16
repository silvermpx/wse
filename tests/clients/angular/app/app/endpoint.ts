// The WSE server's URL, from the page's query (?wse=ws://...): the check
// starts a server of its own. On the server the client takes no connection,
// so the address it is made with there is never dialled.
export function wseEndpoint(): string {
  const given = typeof location === 'undefined' ? null : new URLSearchParams(location.search).get('wse');
  return given ?? 'ws://127.0.0.1:9/wse';
}
