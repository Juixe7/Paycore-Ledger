import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    hot_account_spike: {
      executor: 'ramping-arrival-rate',
      startRate: 100,
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 600,
      stages: [
        { target: 500, duration: '30s' },
        { target: 1000, duration: '60s' },
        { target: 0, duration: '15s' },
      ],
    },
  },
};

const BASE_URL = __ENV.PAYCORE_URL || 'http://localhost:8080';
const API_KEY = __ENV.PAYCORE_API_KEY || '';

export default function () {
  const idemKey = `k6-hot-${uuidv4()}`;
  const res = http.post(
    `${BASE_URL}/v1/payments`,
    JSON.stringify({ amount: 5000, currency: 'INR' }),
    {
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
        'X-Idempotency-Key': idemKey,
      },
    }
  );
  check(res, { 'status is 201': (r) => r.status === 201 });
}
