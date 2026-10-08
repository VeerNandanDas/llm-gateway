/**
 * Lightweight Gateway Concurrency & Load Tester (Pure Node.js)
 * Measures TTFT percentiles (p50, p95), total stream duration, and success rates.
 */

const TARGET_URL = 'http://localhost:3000/v1/chat/completions';
const CONCURRENT_CLIENTS = 10; // Adjust concurrency as needed for your upstream rate limits
const PROMPT = 'Write 3 short bullet points about distributed systems.';

interface StreamMetric {
  id: number;
  success: boolean;
  ttftMs: number;
  totalMs: number;
  chunks: number;
  error?: string;
}

function percentile(arr: number[], p: number): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const index = Math.ceil((p / 100) * sorted.length) - 1;
  return Math.round(sorted[Math.max(0, index)]);
}

async function runClient(id: number): Promise<StreamMetric> {
  const start = performance.now();
  let ttftMs = 0;
  let chunks = 0;

  try {
    const res = await fetch(TARGET_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'qwen/qwen3.8-27b',
        messages: [{ role: 'user', content: PROMPT }],
        stream: true,
      }),
    });

    if (!res.ok) {
      const errText = await res.text();
      return {
        id,
        success: false,
        ttftMs: 0,
        totalMs: performance.now() - start,
        chunks: 0,
        error: `HTTP ${res.status}: ${errText.slice(0, 80)}`,
      };
    }

    const reader = res.body?.getReader();
    if (!reader) throw new Error('No body stream');

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!ttftMs) {
        ttftMs = performance.now() - start;
      }
      chunks++;
    }

    const totalMs = performance.now() - start;
    return { id, success: true, ttftMs, totalMs, chunks };
  } catch (err: any) {
    return {
      id,
      success: false,
      ttftMs: 0,
      totalMs: performance.now() - start,
      chunks: 0,
      error: err.message,
    };
  }
}

async function main() {
  try {
    await fetch(TARGET_URL);
  } catch {
    console.error('\n❌ ERROR: Cannot connect to http://localhost:3000 (Connection Refused)');
    console.error('👉 The gateway server is not currently running.\n');
    console.error('Please start the server first in another terminal:');
    console.error('   npm run dev\n');
    process.exit(1);
  }

  console.log(`🚀 Starting Concurrency Load Test: ${CONCURRENT_CLIENTS} simultaneous streams...`);
  const overallStart = performance.now();

  const promises: Promise<StreamMetric>[] = [];
  for (let i = 1; i <= CONCURRENT_CLIENTS; i++) {
    promises.push(runClient(i));
  }

  const results = await Promise.all(promises);
  const overallDuration = (performance.now() - overallStart) / 1000;

  const successful = results.filter((r) => r.success);
  const failed = results.filter((r) => !r.success);

  const ttfts = successful.map((r) => r.ttftMs);
  const totals = successful.map((r) => r.totalMs);

  console.log('\n==============================================');
  console.log('📊 CONCURRENCY TEST SUMMARY');
  console.log('==============================================');
  console.log(`Total Requests:      ${CONCURRENT_CLIENTS}`);
  console.log(`Successful Streams:  ${successful.length} (${((successful.length / CONCURRENT_CLIENTS) * 100).toFixed(1)}%)`);
  console.log(`Failed Streams:      ${failed.length}`);
  console.log(`Total Wall Time:     ${overallDuration.toFixed(2)}s`);
  console.log('----------------------------------------------');
  console.log(`TTFT p50:            ${percentile(ttfts, 50)} ms`);
  console.log(`TTFT p95:            ${percentile(ttfts, 95)} ms`);
  console.log(`Total Duration p50:  ${percentile(totals, 50)} ms`);
  console.log(`Total Duration p95:  ${percentile(totals, 95)} ms`);
  console.log('==============================================\n');

  if (failed.length > 0) {
    console.log('Sample failure reason(s):');
    failed.slice(0, 3).forEach((f) => console.log(` - Client ${f.id}: ${f.error}`));
    console.log('');
  }
}

main();
