<?php
declare(strict_types=1);
namespace Showcase\Webhooks;

use InvalidArgumentException;
use RuntimeException;

/** HMAC verification, idempotent ingestion, leasing, and bounded retries. */
final class Processor
{
    private const MAX_BODY = 65536;
    private const ALLOWED_SKEW = 300;
    private const MAX_ATTEMPTS = 5;
    private const LEASE_SECONDS = 60;

    public function __construct(private readonly Queue $queue, private readonly string $secret)
    {
        if (strlen($secret) < 16) { throw new InvalidArgumentException('WEBHOOK_SECRET must be at least 16 bytes'); }
    }

    public function ingest(string $body, string $timestamp, string $signature, int $now): array
    {
        if ($body === '' || strlen($body) > self::MAX_BODY) { throw new InvalidArgumentException('Body length outside accepted range'); }
        if (!preg_match('/^\d{10}$/D', $timestamp) || abs($now - (int)$timestamp) > self::ALLOWED_SKEW) {
            throw new Unauthorized('Timestamp is outside the allowed window');
        }
        if (!preg_match('/^[a-f0-9]{64}$/D', $signature)) { throw new Unauthorized('Invalid signature format'); }
        $expected = hash_hmac('sha256', $timestamp . '.' . $body, $this->secret);
        if (!hash_equals($expected, $signature)) { throw new Unauthorized('Invalid message signature'); }
        $decoded = json_decode($body, true, 16, JSON_THROW_ON_ERROR);
        if (!is_array($decoded)) { throw new InvalidArgumentException('Expected a JSON object'); }
        $id = $decoded['id'] ?? null;
        $type = $decoded['type'] ?? null;
        if (!is_string($id) || !preg_match('/^[A-Za-z0-9_.:-]{1,100}$/D', $id) || !is_string($type) || !preg_match('/^[A-Za-z0-9_.:-]{1,100}$/D', $type)) {
            throw new InvalidArgumentException('Event requires safe id and type strings');
        }
        return $this->queue->transaction(static function (array &$items) use ($id, $type, $decoded, $now): array {
            if (isset($items[$id])) { return ['id' => $id, 'duplicate' => true, 'status' => $items[$id]['status']]; }
            $items[$id] = ['id' => $id, 'type' => $type, 'payload' => $decoded['payload'] ?? null,
                'status' => 'queued', 'attempts' => 0, 'next_at' => $now, 'created_at' => $now, 'last_error' => null, 'lease_until' => null];
            return ['id' => $id, 'duplicate' => false, 'status' => 'queued'];
        });
    }

    /** $deliver receives one validated event. Throw on transport errors. */
    public function work(int $now, callable $deliver, int $limit = 10): array
    {
        if ($limit < 1 || $limit > 100) { throw new InvalidArgumentException('limit must be between 1 and 100'); }
        $stats = ['delivered' => 0, 'retried' => 0, 'dead_lettered' => 0];
        for ($i = 0; $i < $limit; $i++) {
            $event = $this->claim($now);
            if ($event === null) { break; }
            try {
                $deliver($event);
                $this->finish($event['id'], true, '', $now);
                $stats['delivered']++;
            } catch (\Throwable $e) {
                $state = $this->finish($event['id'], false, $e->getMessage(), $now);
                $stats[$state === 'dead_letter' ? 'dead_lettered' : 'retried']++;
            }
        }
        return $stats;
    }

    public function get(string $id): ?array
    {
        return $this->queue->transaction(static fn (array &$items): ?array => $items[$id] ?? null);
    }

    private function claim(int $now): ?array
    {
        return $this->queue->transaction(static function (array &$items) use ($now): ?array {
            foreach ($items as &$item) {
                if ($item['status'] === 'processing' && ($item['lease_until'] ?? PHP_INT_MAX) <= $now) {
                    $item['status'] = 'queued';
                    $item['next_at'] = $now;
                }
                if ($item['status'] !== 'queued' || $item['next_at'] > $now) { continue; }
                $item['status'] = 'processing';
                $item['lease_until'] = $now + self::LEASE_SECONDS;
                return $item;
            }
            return null;
        });
    }

    private function finish(string $id, bool $successful, string $error, int $now): string
    {
        return $this->queue->transaction(static function (array &$items) use ($id, $successful, $error, $now): string {
            if (!isset($items[$id])) { throw new RuntimeException('Claimed event no longer exists'); }
            $item =& $items[$id];
            if ($item['status'] !== 'processing') { throw new RuntimeException('Claimed event no longer processing'); }
            $item['attempts']++;
            $item['lease_until'] = null;
            if ($successful) { $item['status'] = 'delivered'; return 'delivered'; }
            $item['last_error'] = substr($error, 0, 200);
            if ($item['attempts'] >= self::MAX_ATTEMPTS) { $item['status'] = 'dead_letter'; return 'dead_letter'; }
            $item['status'] = 'queued';
            $item['next_at'] = $now + min(3600, 5 * (2 ** ($item['attempts'] - 1)));
            return 'queued';
        });
    }
}

final class Unauthorized extends RuntimeException {}
