/**
 * Comprehensive Gateway Test Suite
 * Tests streaming, TTFT, client abort, and error status codes.
 */

const BASE_URL = 'http://localhost:3000';

async function testStreaming() {
  console.log('\n--- 1. Testing Live Token Streaming ---');
  const start = performance.now();
  let firstTokenTime = 0;
  let tokenCount = 0;

  const res = await fetch(`${BASE_URL}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'qwen/qwen3.8-27b',
      messages: [{ role: 'user', content: 'Count from 1 to 5 with commas.' }],
      stream: true,
    }),
  });

  if (!res.ok) {
    const error = await res.text();
    console.error(`❌ Request failed with status ${res.status}:`, error);
    return;
  }

  console.log(`HTTP Status: ${res.status} ${res.statusText}`);
  console.log(`Content-Type: ${res.headers.get('content-type')}`);
  process.stdout.write('Stream Output: "');

  const reader = res.body?.getReader();
  const decoder = new TextDecoder();

  if (!reader) {
    console.error('❌ No readable stream body received');
    return;
  }

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    const chunk = decoder.decode(value, { stream: true });
    if (!firstTokenTime) {
      firstTokenTime = performance.now() - start;
    }

    // Parse SSE lines
    const lines = chunk.split('\n');
    for (const line of lines) {
      if (line.startsWith('data: ') && !line.includes('[DONE]')) {
        try {
          const parsed = JSON.parse(line.slice(6));
          const content = parsed.choices?.[0]?.delta?.content || '';
          if (content) {
            process.stdout.write(content);
            tokenCount++;
          }
        } catch {
          // ignore partial json chunk boundaries in display
        }
      }
    }
  }

  console.log('"');
  const total = performance.now() - start;
  console.log(`✅ Success! TTFT: ${Math.round(firstTokenTime)}ms | Total Time: ${Math.round(total)}ms | Tokens: ~${tokenCount}`);
}

async function testClientAbort() {
  console.log('\n--- 2. Testing Early Client Disconnect (Abort) ---');
  const ac = new AbortController();

  try {
    const fetchPromise = fetch(`${BASE_URL}/v1/chat/completions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'qwen/qwen3.8-27b',
        messages: [{ role: 'user', content: 'Write a 500 word essay about oceans.' }],
        stream: true,
      }),
      signal: ac.signal,
    });

    // Abort after 250ms mid-stream
    setTimeout(() => {
      console.log('⚡ Client aborting stream mid-generation...');
      ac.abort();
    }, 250);

    const res = await fetchPromise;
    const reader = res.body?.getReader();
    while (reader) {
      const { done } = await reader.read();
      if (done) break;
    }
  } catch (err: any) {
    if (err.name === 'AbortError') {
      console.log('✅ Client fetch cleanly aborted as expected. Check gateway console for [Info] Client disconnected.');
    } else {
      console.log('Stream ended or error caught:', err.message);
    }
  }
}

async function testValidationErrors() {
  console.log('\n--- 3. Testing Edge Cases & Status Codes ---');

  // Test 404 for invalid path/method
  const getRes = await fetch(`${BASE_URL}/v1/chat/completions`);
  console.log(`GET /v1/chat/completions -> Status ${getRes.status} (Expected: 404) ${getRes.status === 404 ? '✅' : '❌'}`);

  // Test 400 for invalid JSON
  const badJsonRes = await fetch(`${BASE_URL}/v1/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: 'invalid-json-payload',
  });
  console.log(`Invalid JSON payload -> Status ${badJsonRes.status} (Expected: 400) ${badJsonRes.status === 400 ? '✅' : '❌'}`);
}

async function isServerRunning(): Promise<boolean> {
  try {
    await fetch(`${BASE_URL}/v1/chat/completions`);
    return true;
  } catch (err: any) {
    return false;
  }
}

async function run() {
  console.log('==============================================');
  console.log('🧪 Starting LLM Gateway Verification Tests');
  console.log('==============================================');

  const running = await isServerRunning();
  if (!running) {
    console.error('\n❌ ERROR: Cannot connect to http://localhost:3000 (Connection Refused)');
    console.error('👉 The gateway server is not currently running.\n');
    console.error('Please start the server first in another terminal:');
    console.error('   npm run dev\n');
    console.error('Then re-run this test in this terminal:');
    console.error('   npm test\n');
    process.exit(1);
  }

  await testStreaming();
  await testClientAbort();
  await testValidationErrors();
  console.log('\n==============================================');
  console.log('🎉 All tests completed!');
  console.log('==============================================\n');
}

run();
