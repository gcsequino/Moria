import test from 'node:test';
import { graphCustody } from './fixtures/native-retention/graph-custody.js';

test(
  'qualified v3 Catalog heads survive exact source collection and new-prefix recovery',
  { timeout: 300000 },
  async (t) => {
    await graphCustody(t, true);
  },
);
