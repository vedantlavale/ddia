## Summary

- HTTP server implemented in `ddia/caching/index.ts`.
- Redis client in `ddia/caching/redis.ts`.
- Postgres Pool in `ddia/caching/db.ts`.
- Primary endpoints:
  - `GET /ping` — demonstrates a Postgres query and simple Redis set/get.
  - `GET /users` — returns all users, cached under `users:all`.
  - `GET /users/:id` — returns one user, cached under `users:{id}`.

## Endpoints & Behavior

### GET /ping

- Purpose: small smoke-test demonstrating both Redis and Postgres usage.
- Behavior:
  - Executes `SELECT NOW()` against Postgres.
  - Writes `test = "hello"` to Redis and reads it back.
- Typical JSON response:

```json
{
  "pong": { "now": "2026-03-21T..." },
  "test": "hello"
}
```

(Exact `pong` content depends on your Postgres `NOW()` value.)

### GET /users

- Cache key: `users:all`
- Cache TTL: 30 seconds (set via `redis.set(cacheKey, value, "EX", 30)`).
- Flow:
  1. Check Redis: `const cached = await redis.get(cacheKey)`.
  2. If `cached` exists: log `Cache hit` and return `JSON.parse(cached)`.
  3. If not: log `Cache miss`, simulate latency (~2s), query Postgres: `SELECT * FROM users`, store `JSON.stringify(result.rows)` in Redis with TTL 30s, return rows.
- Notes:
  - Simulated latency is implemented in code to make cache hits/misses observable: `await new Promise(r => setTimeout(r, 2000))`.
  - Stored values are JSON strings, so responses are native JS objects after parsing.

### GET /users/:id

- Cache key pattern: `users:{id}` (e.g. `users:42`)
- Cache TTL: 30 seconds (same as above).
- Flow:
  1. Check Redis for `users:{id}`.
  2. If present: `Cache hit` — return parsed value immediately.
  3. If absent: `Cache miss` — simulate latency (~2s), query Postgres: `SELECT * FROM users WHERE id = $1`, store `JSON.stringify(result.rows[0])` with TTL 30s, return the single row.

## Where to look in the code

- Request handlers and caching logic: `ddia/caching/index.ts`
- Redis client configuration: `ddia/caching/redis.ts`
- Postgres connection pool: `ddia/caching/db.ts`
- Docker wiring (optional reference if present): `ddia/caching/docker-compose.yml`
- Package metadata / scripts: `ddia/caching/package.json`

## Implementation notes / rationale

- Cache keys are explicit and simple:
  - List: `users:all`
  - Single: `users:{id}`
- TTL is short (30s) to keep the demo simple and reproducible.
- JSON serialization is used for simplicity and portability.
- Simulated request latency (~2s delay) is intentionally added on cache-miss paths to make the performance difference clear during testing.
- Error handling in endpoints logs the error and returns HTTP `500` with a generic message.

## Example requests (how to observe behavior)

1. GET all users (first request — miss):

```bash
curl -s http://localhost:3000/users | jq
# Server logs: "Cache miss"
# Response (after ~2s): JSON array of users
```

2. GET all users (second request within 30s — hit):

```bash
curl -s http://localhost:3000/users | jq
# Server logs: "Cache hit"
# Response: same JSON array, returned quickly
```

3. GET single user (first request — miss):

```bash
curl -s http://localhost:3000/users/1 | jq
# Server logs: "Cache miss"
# Response (after ~2s): JSON object for user id=1
```

4. GET single user again (within 30s — hit):

```bash
curl -s http://localhost:3000/users/1 | jq
# Server logs: "Cache hit"
# Response: returned quickly from Redis
```

5. Ping endpoint:

```bash
curl -s http://localhost:3000/ping | jq
# Demonstrates both Postgres and Redis behavior.
```

## Minimal example SQL (to populate a demo `users` table)

This is an example schema you can use with Postgres to create a tiny dataset for testing:

```sql
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO users (name, email) VALUES
('Alice', 'alice@example.com'),
('Bob', 'bob@example.com'),
('Charlie', 'charlie@example.com');
```

## Testing tips

- After starting the server and ensuring Redis + Postgres are reachable, run the example requests above to observe `Cache miss` (first) vs `Cache hit` (subsequent).
- Use timestamps or logging to measure latency difference between cached and uncached responses.
- If you change DB contents directly, remember cached keys may still contain old data until TTL expires; you can delete the Redis keys or add explicit invalidation logic.

## Possible improvements / extensions

- Invalidation:
  - Add endpoints or DB triggers that invalidate/update `users:all` and `users:{id}` when user data changes.
- Namespacing:
  - Use namespaced keys or prefixes by environment to avoid collisions.
- Granular list keys:
  - For filtered/paginated lists, include query parameters in the cache key (e.g., `users:page=2:limit=50`).
- Sliding TTL or LRU strategies:
  - Refresh TTL on access or use more advanced caching layers depending on read/write patterns.
- Metrics:
  - Add counters (cache hits / misses) and timings to monitor effectiveness.
- Serialization robustness:
  - Store additional metadata (e.g., storedAt, version) to help with debugging.

## Troubleshooting (runtime behavior)

- If responses remain stale:
  - Keys are cached for 30s. Delete the Redis key(s) or add an invalidation path to observe fresh data.
- If a request always hits the DB:
  - Check that Redis returns values (inspect Redis with `redis-cli GET users:all`).
  - Verify keys are stored with the expected TTL and stringified JSON.
- If the server errors when querying DB:
  - Check Postgres connectivity and credentials in `ddia/caching/db.ts`.

## Files of interest (quick reference)

- `ddia/caching/index.ts` — HTTP server, endpoints, caching logic.
- `ddia/caching/redis.ts` — `ioredis` client config.
- `ddia/caching/db.ts` — Postgres `Pool` config.
- `ddia/caching/docker-compose.yml` — local Redis/Postgres wiring (if present).
- `ddia/caching/package.json` — scripts and dependencies.

---
