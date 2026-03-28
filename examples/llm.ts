/**
 * Shared LLM helper for examples.
 * Uses OpenRouter with free models. Set OPENROUTER_API_KEY env var.
 */

const API_KEY = process.env.OPENROUTER_API_KEY;
if (!API_KEY) {
  console.error('Set OPENROUTER_API_KEY env var (free key from openrouter.ai/keys)');
  process.exit(1);
}

const BASE = 'https://openrouter.ai/api/v1/chat/completions';

export const MODELS = {
  fast: 'liquid/lfm-2.5-1.2b-instruct:free',
  large: 'arcee-ai/trinity-large-preview:free',
  reasoning: 'stepfun/step-3.5-flash:free',
  nano: 'nvidia/nemotron-3-nano-30b-a3b:free',
};

export interface Message {
  role: 'system' | 'user' | 'assistant';
  content: string;
}

export interface LLMResult {
  content: string;
  reasoningContent?: string;
  model: string;
  inputTokens: number;
  outputTokens: number;
  reasoningTokens: number;
  totalTokens: number;
}

export async function chat(model: string, messages: Message[], maxTokens = 150): Promise<LLMResult> {
  const resp = await fetch(BASE, {
    method: 'POST',
    headers: { Authorization: `Bearer ${API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model, messages, max_tokens: maxTokens }),
  });
  if (!resp.ok) throw new Error(`LLM ${resp.status}: ${await resp.text()}`);
  const data = (await resp.json()) as any;
  const msg = data.choices?.[0]?.message;
  const reasoningTokens = data.usage?.reasoning_tokens ?? 0;
  return {
    content: msg?.content ?? msg?.reasoning ?? '[empty]',
    reasoningContent: msg?.reasoning,
    model: data.model ?? model,
    inputTokens: data.usage?.prompt_tokens ?? 0,
    outputTokens: data.usage?.completion_tokens ?? 0,
    reasoningTokens,
    totalTokens: data.usage?.total_tokens ?? 0,
  };
}

export async function* streamChat(
  model: string,
  messages: Message[],
  maxTokens = 200,
): AsyncGenerator<{ type: 'token' | 'done'; content: string; inputTokens?: number; outputTokens?: number }> {
  const resp = await fetch(BASE, {
    method: 'POST',
    headers: { Authorization: `Bearer ${API_KEY}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ model, messages, max_tokens: maxTokens, stream: true }),
  });
  if (!resp.ok) throw new Error(`LLM ${resp.status}`);

  const reader = resp.body!.getReader();
  const decoder = new TextDecoder();
  let full = '';
  let inTok = 0,
    outTok = 0;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    for (const line of decoder.decode(value, { stream: true }).split('\n')) {
      if (!line.startsWith('data: ')) continue;
      const p = line.slice(6).trim();
      if (p === '[DONE]') continue;
      try {
        const c = JSON.parse(p);
        const delta = c.choices?.[0]?.delta?.content;
        if (delta) {
          full += delta;
          yield { type: 'token', content: delta };
        }
        if (c.usage) {
          inTok = c.usage.prompt_tokens ?? inTok;
          outTok = c.usage.completion_tokens ?? outTok;
        }
      } catch {}
    }
  }
  yield { type: 'done', content: full, inputTokens: inTok, outputTokens: outTok };
}

export const CONNECTION = { addresses: [{ host: 'localhost', port: 6379 }] };
