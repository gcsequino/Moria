import type { S3ClientConfig } from '@aws-sdk/client-s3';

/** Internal SDK defaults: never consult ambient profiles or shared credential files. */
export const s3ClientDefaults = Object.freeze({
  defaultsMode: 'standard',
  retryMode: 'standard',
  maxAttempts: 1,
  authSchemePreference: ['aws.auth#sigv4'],
  disableS3ExpressSessionAuth: true,
  requestChecksumCalculation: 'WHEN_SUPPORTED',
  responseChecksumValidation: 'WHEN_SUPPORTED',
  sigv4aSigningRegionSet: [],
  useArnRegion: false,
  useDualstackEndpoint: false,
  useFipsEndpoint: false,
  userAgentAppId: 'moria',
  forcePathStyle: true,
  followRegionRedirects: false,
} satisfies Partial<S3ClientConfig>);
