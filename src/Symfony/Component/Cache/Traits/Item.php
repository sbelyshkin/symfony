<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

use Symfony\Contracts\Cache\ItemInterface;

/**
 * A short namespace-less class to serialize items with metadata.
 *
 * @author Nicolas Grekas <p@tchwork.com>
 *
 * @internal
 */
class Ͼ
{
    public const METADATA_EXPIRY_OFFSET = 1527506807;

    public readonly mixed $value;
    public readonly array $metadata;

    public function __construct(mixed $value, array $metadata)
    {
        $this->value = $value;
        $this->metadata = $metadata;
    }

    public function __serialize(): array
    {
        $pack = pack('VN', (int) (0.1 + ($this->metadata['expiry'] ?: 3674990454) - self::METADATA_EXPIRY_OFFSET), $this->metadata['ctime'] ?? 0);

        if (isset($this->metadata['tags'])) {
            $pack[4] = $pack[4] | "\x80";
        }

        return [$pack => $this->value] + ($this->metadata['tags'] ?? []);
    }

    public function __unserialize(array $data)
    {
        $pack = array_key_first($data);
        $this->value = $data[$pack];

        if ($hasTags = "\x80" === ($pack[4] & "\x80")) {
            unset($data[$pack]);
            $pack[4] = $pack[4] & "\x7F";
        }

        $metadata = unpack('Vexpiry/Nctime', $pack);
        $metadata['expiry'] += self::METADATA_EXPIRY_OFFSET;

        if (!$metadata['ctime']) {
            unset($metadata['ctime']);
        }

        if ($hasTags) {
            $metadata['tags'] = $data;
        }

        $this->metadata = $metadata;
    }
}
