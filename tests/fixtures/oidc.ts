import { createServer } from 'node:http';
import { createHash, randomUUID } from 'node:crypto';
import { generateKeyPair, exportJWK, SignJWT } from 'jose';

/** A real HTTP OIDC issuer for protocol tests, never enabled by runtime config. */
export async function oidcFixture() {
  const keys = await generateKeyPair('RS256'),
    wrongKeys = await generateKeyPair('RS256');
  const jwk = { ...(await exportJWK(keys.publicKey)), kid: 'test', alg: 'RS256', use: 'sig' };
  const codes = new Map<string, URLSearchParams>();
  const state = {
    subject: 'subject-1',
    email: 'snow@example.test',
    mode: '',
    userInfoCalls: 0,
    beforeUserInfo: undefined as undefined | (() => Promise<void>),
  };
  let issuer = '';
  const server = createServer(async (req, res) => {
    const send = (status: number, value: unknown) => {
      res.writeHead(status, { 'content-type': 'application/json' });
      res.end(JSON.stringify(value));
    };
    const url = new URL(req.url!, issuer);
    if (url.pathname === '/.well-known/openid-configuration')
      return send(200, {
        issuer,
        authorization_endpoint: `${issuer}/authorize`,
        token_endpoint: `${issuer}/token`,
        userinfo_endpoint: `${issuer}/userinfo`,
        jwks_uri: `${issuer}/jwks`,
        response_types_supported: ['code'],
        subject_types_supported: ['public'],
        id_token_signing_alg_values_supported: ['RS256'],
        token_endpoint_auth_methods_supported: ['client_secret_post'],
        code_challenge_methods_supported: ['S256'],
      });
    if (url.pathname === '/jwks') return send(200, { keys: [jwk] });
    if (url.pathname === '/authorize') {
      const code = randomUUID();
      codes.set(code, url.searchParams);
      const callback = new URL(url.searchParams.get('redirect_uri')!);
      callback.searchParams.set('code', code);
      callback.searchParams.set('state', url.searchParams.get('state')!);
      res.writeHead(302, { location: callback.href });
      res.end();
      return;
    }
    if (url.pathname === '/token') {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk);
      const input = new URLSearchParams(Buffer.concat(chunks).toString()),
        params = codes.get(input.get('code')!);
      codes.delete(input.get('code')!);
      if (
        !params ||
        input.get('client_secret') !== 'test-secret' ||
        input.get('redirect_uri') !== params.get('redirect_uri') ||
        createHash('sha256')
          .update(input.get('code_verifier') ?? '')
          .digest('base64url') !== params.get('code_challenge')
      )
        return send(400, { error: 'invalid_grant' });
      const jwt = await new SignJWT({
        nonce: state.mode === 'nonce' ? 'wrong' : params.get('nonce'),
      })
        .setProtectedHeader({ alg: 'RS256', kid: 'test' })
        .setSubject(state.subject)
        .setIssuer(state.mode === 'issuer' ? 'https://wrong.test' : issuer)
        .setAudience(state.mode === 'audience' ? 'wrong' : 'test-client')
        .setIssuedAt()
        .setExpirationTime(state.mode === 'expired' ? Math.floor(Date.now() / 1000) - 120 : '1h')
        .sign(state.mode === 'signature' ? wrongKeys.privateKey : keys.privateKey);
      return send(200, {
        access_token: 'test-access-token',
        token_type: 'Bearer',
        expires_in: 3600,
        id_token: jwt,
      });
    }
    if (url.pathname === '/userinfo') {
      await state.beforeUserInfo?.();
      state.userInfoCalls++;
      if (state.mode === 'unavailable') return send(503, { error: 'unavailable' });
      return send(200, {
        sub: state.subject,
        email: state.email,
        email_verified: state.mode !== 'unverified',
        name: 'Snow Reader',
      });
    }
    send(404, {});
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  issuer = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  return { issuer, state, close: () => new Promise<void>((r) => server.close(() => r())) };
}
