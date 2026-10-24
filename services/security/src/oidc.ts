import * as oidc from 'openid-client';

/** Trusted server configuration. Providers never choose Moria grants or principals. */
export interface OidcProvider {
  id: string;
  label: string;
  issuer: string;
  clientId: string;
  clientSecret: string;
  emails: string[];
}
export class OidcAdapter {
  private configuration?: Promise<oidc.Configuration>;
  constructor(
    readonly provider: OidcProvider,
    private readonly testHttp = false,
  ) {
    const url = new URL(provider.issuer);
    if (url.protocol !== 'https:' && !(testHttp && url.hostname === '127.0.0.1'))
      throw new Error('OIDC issuers require HTTPS.');
  }
  private config() {
    return (this.configuration ??= oidc
      .discovery(
        new URL(this.provider.issuer),
        this.provider.clientId,
        this.provider.clientSecret,
        undefined,
        {
          timeout: 10,
          execute: [
            oidc.enableNonRepudiationChecks,
            ...(this.testHttp ? [oidc.allowInsecureRequests] : []),
          ],
        },
      )
      .catch((error) => {
        this.configuration = undefined;
        throw error;
      }));
  }
  async start(redirectUri: string, state: string, nonce: string, verifier: string) {
    return oidc.buildAuthorizationUrl(await this.config(), {
      redirect_uri: redirectUri,
      scope: 'openid email profile',
      state,
      nonce,
      code_challenge: await oidc.calculatePKCECodeChallenge(verifier),
      code_challenge_method: 'S256',
    }).href;
  }
  async finish(url: URL, state: string, nonce: string, verifier: string) {
    const tokens = await oidc.authorizationCodeGrant(await this.config(), url, {
      expectedState: state,
      expectedNonce: nonce,
      pkceCodeVerifier: verifier,
      idTokenExpected: true,
    });
    const claims = tokens.claims()!;
    const profile = await this.profile(tokens.access_token, claims.sub);
    // Google's signed ID token carries the authoritative Workspace domain claim.
    // UserInfo need not repeat hd. Third-party emails require separate ownership proof.
    if (
      this.provider.issuer === 'https://accounts.google.com' &&
      (claims.email_verified !== true ||
        claims.email !== profile.email ||
        (!profile.email.endsWith('@gmail.com') && (typeof claims.hd !== 'string' || !claims.hd)))
    )
      throw new Error('Use a Gmail or Google Workspace account.');
    return {
      ...profile,
      subject: claims.sub,
      accessToken: tokens.access_token,
      seconds: Math.min(3600, tokens.expires_in ?? 300),
    };
  }
  async profile(accessToken: string, subject: string) {
    const user = await oidc.fetchUserInfo(await this.config(), accessToken, subject);
    if (user.email_verified !== true || typeof user.email !== 'string')
      throw new Error('A verified email is required.');
    return {
      email: user.email.toLowerCase(),
      name: typeof user.name === 'string' ? user.name : user.email,
    };
  }
}
