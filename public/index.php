<?php
declare(strict_types=1);
require __DIR__ . '/../src/Queue.php';
require __DIR__ . '/../src/Processor.php';
use Showcase\Webhooks\{Queue, Processor, Unauthorized};

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');
header('X-Content-Type-Options: nosniff');
if (($_SERVER['REQUEST_METHOD'] ?? '') !== 'POST' || parse_url($_SERVER['REQUEST_URI'] ?? '/', PHP_URL_PATH) !== '/webhooks') {
    http_response_code(404); echo '{"error":"not_found"}'; exit;
}
try {
    $service = new Processor(new Queue(getenv('WEBHOOK_QUEUE_PATH') ?: dirname(__DIR__) . '/var/queue.json'), getenv('WEBHOOK_SECRET') ?: '');
    $result = $service->ingest(file_get_contents('php://input') ?: '', $_SERVER['HTTP_X_WEBHOOK_TIMESTAMP'] ?? '',
        $_SERVER['HTTP_X_WEBHOOK_SIGNATURE'] ?? '', time());
    http_response_code($result['duplicate'] ? 200 : 202);
    echo json_encode($result, JSON_THROW_ON_ERROR);
} catch (Unauthorized $e) {
    http_response_code(401); echo '{"error":"unauthorized"}';
} catch (JsonException | InvalidArgumentException $e) {
    http_response_code(422); echo '{"error":"invalid_event"}';
} catch (Throwable $e) {
    http_response_code(500); echo '{"error":"internal_server_error"}';
}
