<?php
declare(strict_types=1);
require __DIR__ . '/../src/Queue.php';
require __DIR__ . '/../src/Processor.php';
use Showcase\Webhooks\{Queue, Processor, Unauthorized};

function ok(bool $v, string $message): void { if (!$v) { throw new RuntimeException($message); } }
$dir = sys_get_temp_dir() . '/webhooks-test-' . bin2hex(random_bytes(5));
$secret = 'test-secret-that-is-long-enough';
$processor = new Processor(new Queue($dir . '/queue.json'), $secret);
$now = 1780000000;
$body = json_encode(['id' => 'evt_1', 'type' => 'contact.updated', 'payload' => ['contact_id' => 12]], JSON_THROW_ON_ERROR);
$sig = hash_hmac('sha256', $now . '.' . $body, $secret);
try {
    try { $processor->ingest($body, (string)($now - 301), $sig, $now); throw new RuntimeException('Accepted replay'); } catch (Unauthorized $e) {}
    try { $processor->ingest($body, (string)$now, str_repeat('0', 64), $now); throw new RuntimeException('Accepted forged signature'); } catch (Unauthorized $e) {}
    $first = $processor->ingest($body, (string)$now, $sig, $now);
    ok($first['status'] === 'queued' && !$first['duplicate'], 'Initial ingestion');
    ok($processor->ingest($body, (string)$now, $sig, $now)['duplicate'], 'Idempotency');
    $run = $processor->work($now, static function (array $e): void { throw new RuntimeException('temporary unavailable'); });
    ok($run['retried'] === 1, 'Retry scheduled');
    ok($processor->work($now, static fn() => null)['delivered'] === 0, 'Backoff honored');
    $run = $processor->work($now + 5, static fn() => null);
    ok($run['delivered'] === 1 && $processor->get('evt_1')['status'] === 'delivered', 'Successful delivery');
    $body2 = json_encode(['id' => 'evt_2', 'type' => 'billing.paid'], JSON_THROW_ON_ERROR);
    $sig2 = hash_hmac('sha256', $now . '.' . $body2, $secret);
    $processor->ingest($body2, (string)$now, $sig2, $now);
    $time = $now;
    for ($i = 0; $i < 5; $i++) {
        $run = $processor->work($time, static function (): void { throw new RuntimeException('still down'); });
        $time += 5 * (2 ** $i);
    }
    ok($run['dead_lettered'] === 1 && $processor->get('evt_2')['attempts'] === 5, 'Dead letter after capped retries');
    echo "PASS: webhook ingestion (HMAC, skew, deduplication, retries, backoff, delivery, dead-letter)\n";
} finally { @unlink($dir . '/queue.json'); @rmdir($dir); }
