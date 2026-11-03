import assert from 'node:assert/strict';
import { test } from 'node:test';
import { scrubEvent, sentryOptions } from '../src/sentry.js';

test('Sentry stays off when SENTRY_DSN is not set', () => {
  assert.equal(sentryOptions({}).enabled, false);
});

test('errors only: no PII, tracing, trace headers, breadcrumbs or transactions', () => {
  const options = sentryOptions({ SENTRY_DSN: 'https://key@o0.ingest.sentry.io/1' });
  assert.equal(options.enabled, true);
  assert.notEqual((options as { sendDefaultPii?: boolean }).sendDefaultPii, true);
  assert.equal(options.tracesSampleRate, undefined);
  assert.deepEqual(options.tracePropagationTargets, []);
  assert.equal(options.maxBreadcrumbs, 0);
  assert.equal(options.beforeSendTransaction(), null);
});

test('events lose the request (URL, headers, Stripe-Signature, body), user, extra and frame locals', () => {
  const event = scrubEvent({
    message: 'boom',
    request: { url: 'https://hooks.example.com/stripe', headers: { 'stripe-signature': 't=1,v1=abc' }, data: '{"customer_email":"x"}' },
    user: { ip_address: '1.2.3.4' },
    breadcrumbs: [{ message: 'fetch' }],
    extra: { body: 'raw' },
    exception: { values: [{ stacktrace: { frames: [{ function: 'handleStripe', vars: { rawBody: '{}' } }] } }] },
  });
  assert.equal(event.message, 'boom');
  for (const key of ['request', 'user', 'breadcrumbs', 'extra'] as const) assert.equal(event[key], undefined, key);
  assert.equal(event.exception?.values?.[0]?.stacktrace?.frames?.[0]?.vars, undefined);
});

test('drops the request path that captureRequestError adds and strips query strings', () => {
  const event = scrubEvent({
    transaction: 'GET /search?q=private&token=secret',
    tags: { url: 'https://example.com/search?q=private', route: '/search?q=private' },
    contexts: {
      nextjs: { request_path: '/search?q=private', router_kind: 'App Router', route_type: 'render' },
      trace: { trace_id: 't' },
      runtime: { name: 'node' },
    },
  });
  assert.equal(event.transaction, 'GET /search');
  assert.deepEqual(event.tags, { route: '/search' });
  assert.deepEqual(event.contexts, { runtime: { name: 'node' } });
  assert.ok(!JSON.stringify(event).includes('private'));
});

test('strips query strings from URLs in the exception message, frames and debug images', () => {
  const event = scrubEvent({
    exception: {
      values: [{
        value: 'Failed to fetch https://example.com/api?token=secret (from /page?q=private#frag)',
        stacktrace: { frames: [{ filename: 'https://example.com/app.js?v=private', abs_path: '/page?q=private' }] },
      }],
    },
    debug_meta: { images: [{ code_file: 'https://example.com/chunk.js?private=1' }] },
  });
  const value = event.exception?.values?.[0];
  assert.equal(value?.value, 'Failed to fetch https://example.com/api (from /page)');
  assert.deepEqual(value?.stacktrace?.frames?.[0], { filename: 'https://example.com/app.js', abs_path: '/page' });
  assert.equal(event.debug_meta?.images?.[0]?.code_file, 'https://example.com/chunk.js');
  assert.ok(!JSON.stringify(event).includes('private'));
});
