import test from 'node:test';
import { graphCustody } from './fixtures/native-retention/graph-custody.js';
import { graphPruneRollback } from './fixtures/native-retention/prune-rollback.js';

test(
  'qualified v3 Catalog heads survive exact source collection and new-prefix recovery',
  { timeout: 300000 },
  async (t) => {
    await graphCustody(t, true);
  },
);

test(
  'native v3 metadata pruning rolls back an actual failed publication atomically',
  { timeout: 120000 },
  graphPruneRollback,
);
