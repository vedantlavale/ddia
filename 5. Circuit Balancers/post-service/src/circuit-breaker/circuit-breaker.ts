import { createClient } from 'redis';

export type State = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

// Internal state variables
let currentState: State = 'CLOSED';
let failures = 0;
let nextAttemptAt = 0;

const THRESHOLD = 3;
const COOLDOWN = 10000;

// Redis Clients (One for talking, one for listening)
const url = process.env.REDIS_URL || 'redis://localhost:6379';
const pub = createClient({ url });
const sub = createClient({ url });

await pub.connect();
await sub.connect();

// Listen for the "Global Trip"
await sub.subscribe('circuit-events', (msg) => {
  if (msg === 'TRIP') {
    console.log(" GLOBAL TRIP: Syncing state to OPEN");
    tripLocal();
  }
});

function tripLocal() {
  currentState = 'OPEN';
  nextAttemptAt = Date.now() + COOLDOWN;
}

export const breaker = {
  check: (): State => {
    if (currentState === 'OPEN' && Date.now() > nextAttemptAt) {
      currentState = 'HALF_OPEN';
    }
    return currentState;
  },

  success: () => {
    failures = 0;
    currentState = 'CLOSED';
    console.log("Circuit is CLOSED (Healthy)");
  },

  failed: async () => {
    failures++;
    console.log(`Failures: ${failures}/${THRESHOLD}`);
    if (failures >= THRESHOLD) {
      tripLocal();
      await pub.publish('circuit-events', 'TRIP');
      console.log("Circuit BLOWN! Moving to OPEN state.");
    }
  }
};
