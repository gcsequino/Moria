import assert from 'node:assert/strict';

/** Exercises the real scoped HTTP listener, including direct requests outside OSDK. */
export async function checkSnowApiSurface(base: string, cookie: string) {
  const request = async (path: string, method = 'GET', cookieValue = cookie, origin = base) => {
    const response = await fetch(base + path, {
      method,
      headers: {
        cookie: cookieValue,
        origin,
        'content-type': 'application/json',
        'x-moria-key': 'operator-key',
        'x-moria-app-key': 'snow-test-key',
        'x-moria-profile': 'operator',
        authorization: 'Bearer forged-admin',
      },
      ...(method === 'GET'
        ? {}
        : {
            body: JSON.stringify({
              requestKey: 'security-probe',
              input: {},
              enabled: true,
              intervalMinutes: 60,
            }),
          }),
    });
    return { status: response.status, data: await response.json() };
  };
  for (const prefix of ['/management/v1/', '/api/management/v1/']) {
    for (const [path, method] of [
      ['jobs', 'GET'],
      ['lineage', 'GET'],
      ['apps', 'GET'],
      ['workspace', 'GET'],
      ['snotel/sync', 'POST'],
      ['weather/sync', 'POST'],
      ['snotel/schedule', 'POST'],
      ['weather/schedule', 'POST'],
      ['jobs/retry', 'POST'],
    ]) {
      const response = await request(prefix + path, method);
      assert.equal(response.status, 404, `${method} ${prefix}${path} must not exist on Snow`);
      assert.deepEqual(Object.keys(response.data), ['error']);
      assert.equal(response.data.error.code, 'NOT_FOUND');
    }
  }
  for (const method of ['POST', 'PUT', 'PATCH', 'DELETE']) {
    const mutation = await request('/api/v1/actions/edit-status/invoke', method);
    assert.equal(mutation.status, 404);
    assert.deepEqual(Object.keys(mutation.data), ['error']);
  }
  for (const path of ['/api/v1/status', '/api/v1/objects/snow-stations/query']) {
    const method = path.endsWith('query') ? 'POST' : 'GET';
    for (const malformedCookie of ['', `${cookie}; ${cookie}`, cookie + '-tampered']) {
      const response = await request(path, method, malformedCookie);
      assert.equal(response.status, 401);
      assert.deepEqual(Object.keys(response.data), ['error']);
    }
    assert.equal((await request(path, method, cookie, 'https://evil.test')).status, 403);
  }
  const assets = await request('/api/v1/objects/assets/query', 'POST');
  assert.equal(assets.status, 404);
  assert.deepEqual(Object.keys(assets.data), ['error']);
}
