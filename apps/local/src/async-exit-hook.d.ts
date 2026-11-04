declare module 'async-exit-hook' {
  const hooks: { unhookEvent(event: 'SIGINT' | 'SIGTERM'): void };
  export default hooks;
}
