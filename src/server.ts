import 'dotenv/config';
import http from 'node:http';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { Agent, setGlobalDispatcher } from 'undici';

// ==========================================
// BLOCK 1: CONNECTION POOLING CONFIGURATION
// ==========================================
const dispatcher = new Agent({
  keepAliveTimeout: 30_000,
  keepAliveMaxTimeout: 60_000,
  connections: 500, // Max 500 concurrent warm sockets to Groq
  pipelining: 1,    // Strictly 1 stream per socket (Zero Head-of-Line Blocking)
});
setGlobalDispatcher(dispatcher);

// ==========================================
// BLOCK 2: GLOBAL STATE & CONFIGURATIONS
// ==========================================
let activeStreams = 0;
let isShuttingDown = false;
const PORT = process.env.PORT ? parseInt(process.env.PORT, 10) : 3000;
const UPSTREAM_URL = process.env.UPSTREAM_URL || 'https://api.groq.com/openai/v1/chat/completions';
const API_KEY = process.env.GROQ_API_KEY || '';

// ==========================================
// BLOCK 3: RETRY WITH EXPONENTIAL BACKOFF & FULL JITTER
// ==========================================
async function fetchWithRetry(url: string, options: RequestInit, maxRetries = 3): Promise<Response> {
  let attempt = 0;
  while (true) {
    try {
      const response = await fetch(url, options);
      if ((response.status === 429 || response.status >= 500) && attempt < maxRetries && !options.signal?.aborted) {
        throw new Error(`Upstream returned status ${response.status}`);
      }
      return response;
    } catch (err: any) {
      if (attempt >= maxRetries || options.signal?.aborted) {
        throw err;
      }
      attempt++;
      // Full Jitter Formula
      const baseDelay = 500 * Math.pow(2, attempt);
      const jitterDelay = Math.random() * Math.min(baseDelay, 6000);
      console.warn(`[Retry Warning] Attempt ${attempt} failed. Retrying in ${Math.round(jitterDelay)}ms...`);
      await new Promise((resolve) => setTimeout(resolve, jitterDelay));
    }
  }
}

// ==========================================
// BLOCK 4: HTTP SERVER & INCOMING ROUTING
// ==========================================
const server = http.createServer(async (req, res) => {
  // Idle timeout guard: agar client 60s tak dead/freeze ho jaye toh socket destroy karo
  req.socket.setTimeout(60_000, () => {
    req.socket.destroy();
  });

  if (isShuttingDown) {
    res.writeHead(503, { 'Content-Type': 'application/json', 'Connection': 'close' });
    res.end(JSON.stringify({ error: 'Server is shutting down' }));
    return;
  }

  if (req.method !== 'POST' || req.url !== '/v1/chat/completions') {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not Found' }));
    return;
  }

  activeStreams++;
  let isStreamFinished = false;
  const decrementActiveStreams = () => {
    if (!isStreamFinished) {
      isStreamFinished = true;
      activeStreams--;
    }
  };

  res.on('close', decrementActiveStreams);
  res.on('finish', decrementActiveStreams);

  // ==========================================
  // BLOCK 5: BODY PARSING & CLIENT ABORT HOOK
  // ==========================================
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(chunk as Buffer);
  }

  let body: any;
  try {
    body = JSON.parse(Buffer.concat(chunks).toString('utf-8'));
  } catch {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Invalid JSON payload' }));
    return;
  }

  body.stream = true;

  const ac = new AbortController();
  res.on('close', () => {
    if (!res.writableFinished) {
      ac.abort();
    }
  });

  const start = performance.now();
  let ttft = 0;
  let status = 0;
  let outputTokensEstimate = 0;

  try {
    // ==========================================
    // BLOCK 6: UPSTREAM API CALL & SSE HEADERS
    // ==========================================
    const upstream = await fetchWithRetry(UPSTREAM_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${API_KEY}`,
      },
      body: JSON.stringify(body),
      signal: ac.signal,
    });

    status = upstream.status;

    if (!upstream.ok) {
      const errorText = await upstream.text();
      res.writeHead(upstream.status, { 'Content-Type': 'application/json' });
      res.end(errorText);
      return;
    }

    if (!upstream.body) {
      throw new Error('Empty response body received from upstream');
    }

    res.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no', // Prevents Nginx from swallowing token chunks
    });

    // ==========================================
    // BLOCK 7: PIPELINE + ASYNC GENERATOR (The Fix)
    // ==========================================
    // Generator jo chunks ke beech baith kar TTFT aur telemetry capture karta hai
    async function* trackMetrics(source: AsyncIterable<Uint8Array>) {
      for await (const chunk of source) {
        if (!ttft) {
          ttft = performance.now() - start; // Pehle chunk par TTFT capture!
        }
        outputTokensEstimate++;
        yield chunk; // Chunk aage client ko do (backpressure automatically handled by yield)
      }
    }

    // Web stream ko Node stream mein convert karo clean pipeline compatibility ke liye
    const nodeReadable = Readable.fromWeb(upstream.body as any);

    // pipeline automatic backpressure, drain, error forwarding aur cleanup sambhalta hai
    await pipeline(
      trackMetrics(nodeReadable),
      res
    );

    // Note: pipeline complete hote hi res.end() khud call kar deta hai!

  } catch (err: any) {
    if (err.name === 'AbortError' || ac.signal.aborted || err.code === 'ERR_STREAM_PREMATURE_CLOSE') {
      console.log('[Info] Client disconnected; stream cleanly terminated.');
    } else {
      console.error('[Error] Proxy stream exception:', err.message);
      if (!res.headersSent) {
        res.writeHead(502, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Bad Gateway / Upstream Failure' }));
      }
    }
  } finally {
    console.log(
      JSON.stringify({
        status,
        ttft_ms: Math.round(ttft),
        total_ms: Math.round(performance.now() - start),
        tokens_est: outputTokensEstimate,
      })
    );
  }
});

// ==========================================
// BLOCK 8: GRACEFUL SHUTDOWN & DRAIN HOOKS
// ==========================================
function gracefulShutdown(signal: string) {
  console.log(`\nReceived ${signal}. Starting graceful connection drain...`);
  isShuttingDown = true;

  server.close(() => {
    console.log('HTTP server closed. All incoming requests blocked.');
    process.exit(0);
  });

  const drainInterval = setInterval(() => {
    console.log(`[Drain] Waiting for ${activeStreams} active LLM streams to finish...`);
    if (activeStreams <= 0) {
      clearInterval(drainInterval);
      console.log('All streams drained successfully. Exiting.');
      process.exit(0);
    }
  }, 500);

  setTimeout(() => {
    console.error('[Timeout] Graceful shutdown deadline (30s) reached. Forcing exit.');
    process.exit(1);
  }, 30_000).unref();
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

server.listen(PORT, () => {
  console.log(`🚀 LLM Gateway Proxy running on http://localhost:${PORT}`);
});