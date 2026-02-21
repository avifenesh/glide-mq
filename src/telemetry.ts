// Optional OpenTelemetry integration for glide-mq.
// @opentelemetry/api is loaded at runtime via try/catch require.
// If the package is not available, all tracing is a no-op.

const TRACER_NAME = 'glide-mq';

// ------------------------------------------------------------------
// Minimal type surface we need from @opentelemetry/api so we don't
// depend on the package at compile time.
// ------------------------------------------------------------------

interface OTelSpan {
  setAttribute(key: string, value: string | number | boolean): this;
  setStatus(status: { code: number; message?: string }): this;
  recordException(error: Error | string): void;
  end(): void;
}

interface OTelTracer {
  startSpan(name: string, options?: unknown): OTelSpan;
}

interface OTelApi {
  trace: {
    getTracer(name: string): OTelTracer;
  };
  SpanStatusCode: {
    OK: number;
    ERROR: number;
  };
}

// ------------------------------------------------------------------
// No-op implementations
// ------------------------------------------------------------------

const NOOP_SPAN: OTelSpan = {
  setAttribute() {
    return this;
  },
  setStatus() {
    return this;
  },
  recordException() {},
  end() {},
};

const NOOP_TRACER: OTelTracer = {
  startSpan() {
    return NOOP_SPAN;
  },
};

// ------------------------------------------------------------------
// Runtime detection
// ------------------------------------------------------------------

let otelApi: OTelApi | null = null;
try {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  otelApi = require('@opentelemetry/api') as OTelApi;
} catch {
  // @opentelemetry/api not installed - tracing is a no-op
}

let userTracer: OTelTracer | null = null;

/**
 * Allow the user to supply their own tracer instance.
 * If not called, the tracer is auto-resolved from @opentelemetry/api.
 */
export function setTracer(tracer: unknown): void {
  userTracer = tracer as OTelTracer;
}

function getTracer(): OTelTracer {
  if (userTracer) return userTracer;
  if (otelApi) return otelApi.trace.getTracer(TRACER_NAME);
  return NOOP_TRACER;
}

/** True when a real OTel API is available (either user-provided or auto-detected). */
export function isTracingEnabled(): boolean {
  return userTracer !== null || otelApi !== null;
}

// ------------------------------------------------------------------
// Span helpers
// ------------------------------------------------------------------

export type SpanAttributes = Record<string, string | number | boolean>;

/**
 * Start a span, execute the callback, and end the span when done.
 * On error the exception is recorded on the span and re-thrown.
 */
export async function withSpan<T>(
  name: string,
  attributes: SpanAttributes,
  fn: (span: OTelSpan) => Promise<T>,
): Promise<T> {
  if (!isTracingEnabled()) {
    return fn(NOOP_SPAN);
  }

  const tracer = getTracer();
  const span = tracer.startSpan(name);

  for (const [key, value] of Object.entries(attributes)) {
    span.setAttribute(key, value);
  }

  try {
    const result = await fn(span);
    span.setStatus({ code: otelApi?.SpanStatusCode.OK ?? 0 });
    return result;
  } catch (err) {
    span.setStatus({
      code: otelApi?.SpanStatusCode.ERROR ?? 1,
      message: err instanceof Error ? err.message : String(err),
    });
    span.recordException(err instanceof Error ? err : new Error(String(err)));
    throw err;
  } finally {
    span.end();
  }
}

/**
 * Start a child span for a sub-operation within an already-active span.
 * Same semantics as withSpan but the callback does not receive the span.
 */
export async function withChildSpan<T>(name: string, attributes: SpanAttributes, fn: () => Promise<T>): Promise<T> {
  return withSpan(name, attributes, () => fn());
}
