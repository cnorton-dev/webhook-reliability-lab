<?php
declare(strict_types=1);
require __DIR__ . '/../src/Queue.php';
require __DIR__ . '/../src/Processor.php';
use Showcase\Webhooks\{Queue, Processor};

$endpoint = getenv('WEBHOOK_FORWARD_URL') ?: '';
$url = filter_var($endpoint, FILTER_VALIDATE_URL) ? parse_url($endpoint) : false;
if (!is_array($url) || ($url['scheme'] ?? '') !== 'https' || empty($url['host']) || isset($url['user']) || isset($url['pass'])) {
    fwrite(STDERR, "Set WEBHOOK_FORWARD_URL to an authorized HTTPS receiver URL.\n"); exit(2);
}
try {
    $service = new Processor(new Queue(getenv('WEBHOOK_QUEUE_PATH') ?: dirname(__DIR__) . '/var/queue.json'), getenv('WEBHOOK_SECRET') ?: '');
    $result = $service->work(time(), static function (array $event) use ($endpoint): void {
        $body = json_encode(['id' => $event['id'], 'type' => $event['type'], 'payload' => $event['payload']], JSON_THROW_ON_ERROR);
        $ctx = stream_context_create(['http' => ['method' => 'POST', 'timeout' => 8, 'ignore_errors' => true,
            'follow_location' => 0, 'header' => "Content-Type: application/json\r\nX-Event-ID: " . $event['id'] . "\r\n",
            'content' => $body]]);
        $response = @file_get_contents($endpoint, false, $ctx);
        $statusLine = $http_response_header[0] ?? '';
        if ($response === false || !preg_match('/^HTTP\/\S+ (2\d\d)\b/', $statusLine)) {
            throw new RuntimeException('Downstream rejected request or did not respond');
        }
    });
    echo json_encode($result, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT) . PHP_EOL;
} catch (Throwable $e) { fwrite(STDERR, "Worker error: " . $e->getMessage() . "\n"); exit(1); }
