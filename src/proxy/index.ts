import type { ProxyOptions } from './types';
import { createRoutes } from './routes';

export type { Express } from 'express';

export type {
  ProxyOptions,
  AddJobRequest,
  AddJobResponse,
  AddJobSkippedResponse,
  AddBulkRequest,
  AddBulkResponse,
  GetJobResponse,
  JobCountsResponse,
  HealthResponse,
  ErrorResponse,
} from './types';

/**
 * Create an Express application that proxies HTTP requests to glide-mq queues.
 *
 * Requires `express` as a peer dependency. Install it separately:
 * ```
 * npm install express
 * ```
 *
 * @example
 * ```typescript
 * import { createProxyServer } from 'glide-mq/proxy';
 *
 * const proxy = createProxyServer({
 *   connection: { addresses: [{ host: 'localhost', port: 6379 }] },
 * });
 *
 * proxy.app.listen(3000, () => console.log('Proxy listening on :3000'));
 *
 * // On shutdown:
 * await proxy.close();
 * ```
 */
export function createProxyServer(opts: ProxyOptions): {
  app: import('express').Express;
  close: () => Promise<void>;
} {
  if (!opts.connection && !opts.client) {
    throw new Error('ProxyOptions requires either `connection` or `client`');
  }

  let express: typeof import('express');
  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    express = require('express');
  } catch {
    throw new Error('express is required for glide-mq/proxy. Install it: npm install express');
  }

  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '1mb' }));

  const { router, closeQueues } = createRoutes(opts, () => express.Router());
  app.use(router);

  // Consistent JSON error responses for Express-level errors (malformed JSON, payload too large)
  app.use((err: any, _req: any, res: any, _next: any) => {
    const status = err.status || err.statusCode || 500;
    res.status(status).json({ error: err.type === 'entity.too.large' ? 'Payload too large' : 'Bad request' });
  });

  return {
    app,
    close: closeQueues,
  };
}
