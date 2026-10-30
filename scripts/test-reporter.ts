import { Readable } from 'node:stream';
import { spec, type TestEvent } from 'node:test/reporters';

export async function* requireTestCases(source: AsyncIterable<TestEvent>) {
  for await (const event of source) {
    if (
      event.type === 'test:pass' &&
      /(?:^|\/)tests\/.*\.test\.ts$/.test(event.data.name.replaceAll('\\', '/'))
    )
      throw new Error(`Test module exited without reporting cases: ${event.data.name}`);
    yield event;
  }
}

export default async function* report(source: AsyncIterable<TestEvent>) {
  yield* Readable.from(requireTestCases(source)).compose(new spec());
}
