import { compileManifest } from '../../../packages/compiler/src/browser.js';
import fixture from '../../../packages/contracts/compiler-conformance.json' with { type: 'json' };
const target = document.querySelector('#result')!;
try {
  let blocked = false;
  try {
    new Function('return 1')();
  } catch {
    blocked = true;
  }
  if (!blocked) throw new Error('Dynamic code generation was not blocked by CSP');
  let passed = 0;
  for (const item of fixture.cases) {
    let accepted = true;
    try {
      await compileManifest(item.manifest, { principalAttributes: item.attributes as any });
    } catch {
      accepted = false;
    }
    if (accepted !== item.valid) throw new Error(`Conformance failed: ${item.name}`);
    passed++;
  }
  target.textContent = `PASS ${passed}/${fixture.cases.length} compiler cases. Dynamic code generation blocked. WebCrypto fingerprints enabled.`;
} catch (error) {
  target.textContent = `FAIL ${error instanceof Error ? error.message : error}`;
}
