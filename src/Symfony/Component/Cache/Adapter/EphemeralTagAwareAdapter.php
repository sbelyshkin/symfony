<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Cache\Adapter;

use Psr\Cache\CacheItemInterface;
use Psr\Cache\CacheItemPoolInterface;
use Symfony\Component\Cache\CacheItem;

/**
 * Base implementation of AbstractEphemeralTagAwareAdapter::class.
 *
 * Allows to leverage any PSR-6 compatible adapter for accessing cache storage and tagging items.
 * Defines an opinionated formats and algorithms for generating tag versions and packing item's value,
 * tag versions and item's metadata into a single value to be stored.
 *
 * Provides passive Optimistic Concurrency Control by allowing deferred computation of item's value
 * which starts only after obtaining attached tags' versions.
 *
 * @link https://en.wikipedia.org/wiki/Optimistic_concurrency_control
 * @link https://en.wikipedia.org/wiki/Load-link/store-conditional
 *
 * @author Sergey Belyshkin <sbelyshkin@gmail.com>
 */
class EphemeralTagAwareAdapter extends AbstractEphemeralTagAwareAdapter
{
    /**
     * @var string
     */
    protected $instanceId = '';
    /**
     * @var string[]
     */
    private $lastRetrievedTagVersions = [];
    /**
     * @var \Closure
     */
    private $computeAndPackItems;

    /**
     *
     * @param CacheItemPoolInterface $itemPool
     * @param CacheItemPoolInterface|null $tagPool
     */
    public function __construct(CacheItemPoolInterface $itemPool, CacheItemPoolInterface $tagPool = null)
    {
        parent::__construct($itemPool, $tagPool);
        $this->setCallbackWrapper(null);
        $this->instanceId = \pack('N', \crc32(\getmypid() . '@' . \gethostname()));

        $getPrefixedKeyMethod = \Closure::fromCallable([$this, 'getPrefixedKey']);
        $this->computeAndPackItems = \Closure::bind(
            static function ($deferred, $tagVersions) use ($getPrefixedKeyMethod) {
                $packedItems = [];
                foreach ($deferred as $key => $item) {
                    $startTime = \microtime(true);
                    $key = (string) $key;
                    $itemTagVersions = [];
                    $metadata = $item->newMetadata;
                    if (isset($metadata[CacheItem::METADATA_TAGS])) {
                        foreach ($metadata[CacheItem::METADATA_TAGS] as $tag) {
                            if (!isset($tagVersions[$tag])) {
                                // Don't compute the value
                                if ($item->value instanceof \Closure) {
                                    $item->value = null;
                                }
                                // Don't save items without full set of valid tags
                                continue 2;
                            }
                            $itemTagVersions[$tag] = $tagVersions[$tag];
                        }
                        unset($metadata[CacheItem::METADATA_TAGS]);
                    }
                    // Compute the value in case it's passed as a callback function
                    if ($item->value instanceof \Closure) {
                        $item->value = ($item->value)();
                    }
                    // Pack the value, tags and meta data.
                    $value = ['$' => $item->value];
                    if ($itemTagVersions) {
                        $value['#'] = $itemTagVersions;
                    }
                    if ($metadata) {
                        // Update item's creation time to represent real computation time
                        $ctime = $item->newMetadata[CacheItem::METADATA_CTIME] += (int) \ceil(1000 * (\microtime(true) - $startTime));
                        // 1. 03:14:08 UTC on Tuesday, 19 January 2038 timestamp will reach 0x7FFFFFFF and 32-bit systems
                        // will go back to Unix Epoch, but on 64-bit systems it's OK to use first 32 bits of timestamp
                        // till 06:28:15 UTC on Sunday, 7 February 2106, when it'll reach 0xFFFFFFFF.
                        // 2. CTIME is packed as an 8/16/24/32-bits integer. For reference, 24 bits are able to reflect
                        // intervals up to 4 hours 39 minutes 37 seconds and 215 ms, but in most cases 8 bits are enough.
                        $length = 4 + ($ctime <= 255 ? 1 : ($ctime <= 65535 ? 2 : ($ctime <= 16777215 ? 3 : 4)));
                        $value['^'] = \substr(\pack('NV', (int) \ceil($metadata[CacheItem::METADATA_EXPIRY]), $ctime), 0, $length);
                    }

                    $item->metadata = $item->newMetadata;

                    $packedItem = new CacheItem();
                    $packedItem->key = $getPrefixedKeyMethod($key);
                    $packedItem->value = $value;
                    $packedItem->expiry = $item->expiry;

                    $packedItems[$key] = $packedItem;
                }

                return $packedItems;
            },
            null,
            CacheItem::class
        );

    }

    /**
     * {@inheritdoc}
     */
    public function getItems(array $keys = []): array
    {
        // Force retrieving the tags
        $this->clearLastRetrievedTagVersions();

        return parent::getItems($keys);
    }

    /**
     * {@inheritdoc}
     */
    public function commit(): bool
    {
        $result = parent::commit();
        $this->clearLastRetrievedTagVersions();

        return $result;
    }

    /**
     * {@inheritdoc}
     */
    public function clear(string $prefix = ''): bool
    {
        self::clearLastRetrievedTagVersions();

        return parent::clear($prefix);
    }

    /**
     * {@inheritdoc}
     */
    public function invalidateTags(array $tags): bool
    {
        self::clearLastRetrievedTagVersions();

        return parent::invalidateTags($tags);
    }

    /**
     * @throws \Psr\Cache\InvalidArgumentException
     */
    public function __destruct()
    {
        $this->commit();
    }

    /**
     * Checks if the structure of the given value meets the format used for packed values.
     *
     * @param $value
     * @return bool
     */
    protected function isPackedValueStructureValid($value): bool
    {
        return \is_array($value) && ((['$'] === ($keys = \array_keys($value)))
                || ((['$', '#'] === $keys || ['$', '#', '^'] === $keys && \is_string($value['^'])) && \is_array($value['#'])));
    }

    /**
     * {@inheritdoc}
     */
    protected function packItems(iterable $items, array $tagVersions): array
    {
        return ($this->computeAndPackItems)($items, $tagVersions);
    }

    /**
     * {@inheritdoc}
     */
    protected function unpackItem(CacheItemInterface $item): array
    {
        $value = $item->get();

        if (!$this->isPackedValueStructureValid($value)) {
            return [];
        }

        $unpacked = [
            'value' => $value['$'],
            'tagVersions' => $value['#'] ?? [],
            'meta' => [],
        ];

        if (isset($value['^'])) {
            $m = \unpack('Ne/Vc', \str_pad($value['^'], 8, "\x00"));
            $metadata[CacheItem::METADATA_EXPIRY] = $m['e'];
            $metadata[CacheItem::METADATA_CTIME] = $m['c'];
            $unpacked['meta'] = $metadata;
        }

        if ($unpacked['tagVersions']) {
            $tags = \array_keys($unpacked['tagVersions']);
            $unpacked['meta'][CacheItem::METADATA_TAGS] = \array_combine($tags, $tags);
        }

        return $unpacked;
    }

    /**
     * Clears internal tag cache.
     *
     * It's allowed to override this method in order to reduce the number of round trips.
     */
    protected function clearLastRetrievedTagVersions(): void
    {
        $this->lastRetrievedTagVersions = [];
    }

    /**
     * Gets tags from a tag storage or from its own "cache".
     *
     * May return only a part of requested tags or even none of them in case of technical issues.
     *
     * @param array $tags
     *
     * @throws \Psr\Cache\InvalidArgumentException
     *
     * @return string[]
     */
    protected function getTagVersions(array $tags): array
    {
        if (!$tags) {
            return [];
        }

        if ($this->lastRetrievedTagVersions) {
            $tagVersions = \array_intersect_key($this->lastRetrievedTagVersions, \array_flip($tags));
            if (\count($tagVersions) === \count($tags)) {
                // All requested tags are in the last retrieved set
                return $tagVersions;
            }
        }

        $this->lastRetrievedTagVersions = $this->retrieveTagVersions($tags);

        return $this->lastRetrievedTagVersions;
    }

    /**
     * Generates unique string for robust tag versioning
     *
     * @return string
     */
    protected function generateTagVersion(): string
    {
        // Add an instance ID to preclude the possibility of ABA problem
        return \pack('N', \mt_rand()) . $this->instanceId;
    }

}
