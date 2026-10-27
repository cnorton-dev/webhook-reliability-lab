<?php
declare(strict_types=1);
namespace Showcase\Webhooks;

use RuntimeException;

/** Durable local queue: file-locked, single-host reference adapter. */
final class Queue
{
    public function __construct(private readonly string $path) {}

    public function transaction(callable $callback): mixed
    {
        $dir = dirname($this->path);
        if (!is_dir($dir) && !mkdir($dir, 0700, true) && !is_dir($dir)) { throw new RuntimeException('Unable to create queue directory'); }
        $handle = fopen($this->path, 'c+b');
        if ($handle === false) { throw new RuntimeException('Unable to open queue'); }
        try {
            if (!flock($handle, LOCK_EX)) { throw new RuntimeException('Unable to lock queue'); }
            rewind($handle);
            $text = stream_get_contents($handle);
            if ($text === false) { throw new RuntimeException('Unable to read queue'); }
            $events = trim($text) === '' ? [] : json_decode($text, true, 512, JSON_THROW_ON_ERROR);
            if (!is_array($events)) { throw new RuntimeException('Corrupt queue state'); }
            $result = $callback($events);
            $json = json_encode($events, JSON_THROW_ON_ERROR | JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
            if (!ftruncate($handle, 0) || !rewind($handle) || fwrite($handle, $json) !== strlen($json) || !fflush($handle)) {
                throw new RuntimeException('Unable to persist queue');
            }
            chmod($this->path, 0600);
            return $result;
        } finally { fclose($handle); }
    }
}
