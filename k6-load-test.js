import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Counter } from 'k6/metrics';

// Custom Metrics for AI Gateway Observability
const ttftTrend = new Trend('ttft_ms');
const totalDurationTrend = new Trend('stream_total_duration_ms');
const successfulStreams = new Counter('successful_streams');
const errorStreams = new Counter('error_streams');

export const options = {
  scenarios: {
    safe_streaming_load: {
      executor: 'shared-iterations',
      vus: 2,
      iterations: 10,         // Stays safely within Groq Free-Tier 30 RPM limit
      maxDuration: '30s',
    },
  },
  thresholds: {
    // Quality Gates / SLA targets
    'stream_total_duration_ms': ['p(50)<3000', 'p(95)<8000', 'p(99)<15000'],
    'http_req_failed': ['rate<0.01'], // < 1% error rate allowed under 500 streams
  },
};

export default function () {
  const url = 'http://localhost:3000/v1/chat/completions';
  
  const payload = JSON.stringify({
    model: 'qwen/qwen3.8-27b',
    messages: [
      {
        role: 'user',
        content: 'Say hello in 5 words.',
      },
    ],
    stream: true,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
    responseType: 'text',
    timeout: '60s',
  };

  const startTime = Date.now();
  const res = http.post(url, payload, params);
  const totalDuration = Date.now() - startTime;

  totalDurationTrend.add(totalDuration);

  const isOk = check(res, {
    'status is 200': (r) => r.status === 200,
    'content-type is text/event-stream': (r) =>
      r.headers['Content-Type'] && r.headers['Content-Type'].includes('text/event-stream'),
    'contains DONE marker': (r) => r.body && r.body.includes('[DONE]'),
  });

  if (isOk) {
    successfulStreams.add(1);
  } else {
    errorStreams.add(1);
  }

  // Small pause between stream iterations per virtual user
  sleep(1);
}
