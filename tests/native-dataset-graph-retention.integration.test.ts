import test from 'node:test';
import { graphCustody } from './fixtures/native-retention/graph-custody.js';

test(
  'graph custody survives exact sidecar collection and independent clean-host recovery',
  { timeout: 300000 },
  async (t) => {
    await graphCustody(t);
  },
);
