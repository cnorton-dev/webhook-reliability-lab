# Webhook Reliability Lab

A reference PHP 8.2+ webhook ingestion and delivery pipeline with authenticated events, deduplication, durable local queueing, retries and dead-letter handling. Intended as an engineering portfolio example, not a drop-in multi-tenant production service.

## Features

- HMAC-SHA256 signature verification over `<timestamp>.<raw body>`
- Five-minute timestamp tolerance to limit replay attempts
- Event ID deduplication with idempotent responses
- Locked JSON queue with worker claim leases
- Retry schedule: 5, 10, 20, 40 seconds (capped after five attempts)
- Dead-letter state for manual investigation
- Separate HTTP ingest, domain processor and worker
- No external dependencies for local development

## Start the receiver

```bash
export WEBHOOK_SECRET='replace-with-at-least-16-secret-bytes'
php -S 127.0.0.1:8081 -t public
```

Send an example signed event:

```bash
BODY='{"id":"evt_100","type":"customer.updated","payload":{"customer_id":42}}'
STAMP=$(date +%s)
SIGNATURE=$(php -r 'echo hash_hmac("sha256", $argv[1] . "." . $argv[2], getenv("WEBHOOK_SECRET"));' "$STAMP" "$BODY")
curl -i http://127.0.0.1:8081/webhooks \
  -H "X-Webhook-Timestamp: $STAMP" \
  -H "X-Webhook-Signature: $SIGNATURE" \
  -H 'Content-Type: application/json' \
  --data "$BODY"
```

To process queued events, configure an **authorized HTTPS destination** and run the one-shot worker:

```bash
export WEBHOOK_FORWARD_URL='https://your-receiver.example/events'
php bin/worker.php
```

Schedule it with your own cron or supervisor in a local test setup. Do **not** point it at an untrusted receiver.

## Test

```bash
php tests/run.php
```

## Design and production limitations

The sample demonstrates the at-least-once delivery model: a downstream recipient **must deduplicate by the `X-Event-ID` header**, since crashes can trigger re-delivery. Current storage is a single-host JSON file using `flock`; production systems should use a transactional SQL database or real message queue with row-level leases, horizontal worker coordination, proper metrics, and authentication between services. Long-running deliveries need lease renewal and fencing before horizontally scaling the worker. Incoming payloads are capped at 64 KiB, and configured outbound URLs are HTTPS-only, but a production worker also needs network egress allowlisting and DNS pinning.

The worker is only an illustration of retry scheduling, not an end-to-end security product. No private customer data or employer source code is included.
