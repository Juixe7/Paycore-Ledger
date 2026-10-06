import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    steady_state: {
      executor: 'constant-arrival-rate',
      rate: 300,
      timeUnit: '1s',
      duration: '2m',
      preAllocatedVUs: 100,
      maxVUs: 400,
    },
  },
  thresholds: {
    'http_req_duration{step:create}': ['p(99)<80'],
    'http_req_duration{step:capture}': ['p(99)<150'],
    'http_req_failed': ['rate<0.005'],
  },
};

const BASE_URL = __ENV.PAYCORE_URL || 'http://localhost:8080';
const API_KEY = __ENV.PAYCORE_API_KEY || '';

export default function () {
  const headers = {
    'Content-Type': 'application/json',
    'X-API-Key': API_KEY,
    'X-Idempotency-Key': `k6-create-${uuidv4()}`,
  };

  // 1. Create payment
  const createRes = http.post(
    `${BASE_URL}/v1/payments`,
    JSON.stringify({ amount: 10000, currency: 'INR' }),
    { headers, tags: { step: 'create' } }
  );
  check(createRes, { 'created 201': (r) => r.status === 201 });
  if (createRes.status !== 201) return;

  const paymentId = JSON.parse(createRes.body).id;

  // 2. Authorize payment
  const authRes = http.post(
    `${BASE_URL}/v1/payments/${paymentId}/authorize`,
    JSON.stringify({}),
    {
      headers: { ...headers, 'X-Idempotency-Key': `k6-auth-${paymentId}` },
      tags: { step: 'authorize' },
    }
  );
  check(authRes, { 'authorized 200': (r) => r.status === 200 });

  // 3. Capture payment
  const capRes = http.post(
    `${BASE_URL}/v1/payments/${paymentId}/capture`,
    JSON.stringify({}),
    {
      headers: { ...headers, 'X-Idempotency-Key': `k6-cap-${paymentId}` },
      tags: { step: 'capture' },
    }
  );
  check(capRes, { 'captured 200': (r) => r.status === 200 });
  sleep(0.05);
}
