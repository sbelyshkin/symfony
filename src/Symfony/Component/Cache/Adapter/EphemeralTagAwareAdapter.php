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
use Symfony\Component\Cache\CacheItem;
use Symfony\Component\Cache\PruneableInterface;
use Symfony\Component\Cache\ResettableInterface;
use Symfony\Component\Cache\Traits\ContractsTrait;
use Symfony\Component\Cache\Traits\ProxyTrait;
use Symfony\Contracts\Cache\TagAwareCacheInterface;

/**
 * This Adapter is designed as a safe cache storage with tag-based invalidation.
 * The safety means the ability to invalidate any items by the known tags they were saved with.
 * In other words, storage is safe from orphan and stale items due to inconsistency in a tag-item relations
 * which may arise when using LRU or other ephemeral caches.
 *
 * If a previous tag version has been evicted or expired (or new one cannot be created),
 * Adapter rejects all items with that tag version (or rejects save operation).
 *
 * This ability does not affected by peak loads and out-of-memory state.
 *
 *
 * @author Sergey Belyshkin <sbelyshkin@gmail.com>
 */
class EphemeralTagAwareAdapter extends AbstractEphemeralTagAwareAdapter
{
    use ContractsTrait;
    use ProxyTrait;

    /**
     * @var string
     */
    protected $tagIdPrefix = '';
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
    private $createCacheItem;
    /**
     * @var \Closure
     */
    private $extractTags;
    /**
     * @var \Closure
     */
    private $computeValues;

    /**
     *
     * @param AdapterInterface $itemPool
     * @param AdapterInterface|null $tagPool
     */
    public function __construct(AdapterInterface $itemPool, AdapterInterface $tagPool = null)
    {
        parent::__construct($itemPool, $tagPool);
        $this->setCallbackWrapper(null);
        $this->instanceId = \pack('L', \crc32(\getmypid() . '@' . \gethostname()));

        $this->extractTags = \Closure::bind(
            static function ($deferred) {
                $uniqueTags = [];
                foreach ($deferred as $item) {
                    $uniqueTags += $item->newMetadata[CacheItem::METADATA_TAGS] ?? [];
                }

                return $uniqueTags;
            },
            null,
            CacheItem::class
        );

        $this->computeValues = \Closure::bind(
            static function ($deferred, $tagVersions) {
                $valuesByKey = [];
                foreach ($deferred as $key => $item) {
                    $startTime = \microtime(true);
                    $key = (string) $key;
                    // Compute the value in case it's passed as a callback function
                    if ($item->value instanceof \Closure) {
                        $item->value = ($item->value)();
                    }
                    // Store Value and Tags on the cache value
                    $metadata = $item->newMetadata;
                    if (isset($metadata[CacheItem::METADATA_TAGS])) {
                        $itemTagVersions = [];
                        foreach ($metadata[CacheItem::METADATA_TAGS] as $tag) {
                            if (!isset($tagVersions[$tag])) {
                                // Don't save items without full set of valid tags
                                continue 2;
                            }
                            $itemTagVersions[$tag] = $tagVersions[$tag];
                        }
                        $value = ['v' => $item->value, 't' => $itemTagVersions];
                        unset($metadata[CacheItem::METADATA_TAGS]);
                    } else {
                        $value = ['v' => $item->value, 't' => []];
                    }

                    if ($metadata) {
                        // Update item's creation time to represent real computation time
                        $item->newMetadata[CacheItem::METADATA_CTIME] += (int) \ceil(1000 * (\microtime(true) - $startTime));
                        // For compactness, expiry and creation duration are packed, using magic numbers as separators
                        $value['m'] = \pack('VN', (int) (0.1 + $metadata[CacheItem::METADATA_EXPIRY] - CacheItem::METADATA_EXPIRY_OFFSET), $item->newMetadata[CacheItem::METADATA_CTIME]);
                    }

                    $packedItem = new CacheItem();
                    $packedItem->key = $key;
                    $packedItem->value = $value;
                    $packedItem->expiry = $item->expiry;

                    $valuesByKey[$key] = $packedItem;
                }

                return $valuesByKey;
            },
            null,
            CacheItem::class
        );

        $this->createCacheItem = \Closure::bind(
            static function ($key, $value, $isHit) {
                $item = new CacheItem();
                $item->key = $key;
                $item->isTaggable = true;
                // The exact structure of the value should be examined before calling this function
                if (!\is_array($value)) {
                    return $item;
                }
                $item->isHit = $isHit;
                // Extract value, tags and meta data from the cache value
                $item->value = $value['v'];
                $tags = \array_keys($value['t']);
                $item->metadata[CacheItem::METADATA_TAGS] = \array_combine($tags, $tags);
                if (isset($value['m'])) {
                    // For compactness these values are packed, & expiry is offset to reduce size
                    $v = \unpack('Ve/Nc', $value['m']);
                    $item->metadata[CacheItem::METADATA_EXPIRY] = $v['e'] + CacheItem::METADATA_EXPIRY_OFFSET;
                    $item->metadata[CacheItem::METADATA_CTIME] = $v['c'];
                    if ($item->metadata[CacheItem::METADATA_EXPIRY] < \microtime(true)) {
                        $item->value = null;
                        $item->isHit = false;
                    }
                }

                return $item;
            },
            null,
            CacheItem::class
        );
    }

    protected function isStructureValid($value): bool
    {
        return \is_array($value) && \count($value) <= 3 && isset($value['v'], $value['t']) && \is_array($value['t']);
    }

    protected function containsInvalidTags($value, array $tagVersions): bool
    {
        return !empty($value['t']) && $value['t'] != \array_intersect_key($tagVersions, $value['t']);
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

        return parent::deleteItems($tags);
    }

    public function __sleep()
    {
        throw new \BadMethodCallException('Cannot serialize '.__CLASS__);
    }

    public function __wakeup()
    {
        throw new \BadMethodCallException('Cannot unserialize '.__CLASS__);
    }

    /**
     * @throws \Psr\Cache\InvalidArgumentException
     */
    public function __destruct()
    {
        $this->commit();
    }

    protected function createCacheItem(string $key, ?array $value, bool $isHit): CacheItem
    {
        return ($this->createCacheItem)($key, $value, $isHit);
    }


    protected function extractTagsFromItems(iterable $items): array
    {
        return ($this->extractTags)($items);
    }

    protected function extractTagVersionsFromValue($value): array
    {
        return $value['t'];
    }

    protected function computeValues(iterable $items, array $tagVersions): array
    {
        return ($this->computeValues)($items, $tagVersions);
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
            if (($tagVersions = array_intersect_key($this->lastRetrievedTagVersions, array_flip($tags))) && count($tagVersions) === count($tags)) {
                // Allow to use recent tags' versions only for the very next operation to ensure their freshness
                $this->lastRetrievedTagVersions = [];
                // All requested tags are in the last retrieved set
                return $tagVersions;
            }
        }

        $this->lastRetrievedTagVersions = $this->retrieveTagVersions($tags);

        return $this->lastRetrievedTagVersions;
    }

    /**
     * Loads tags from or creates new ones in a tag storage.
     *
     * May return only a part of requested tags or even none of them in case of technical issues.
     *
     * @param array $tags
     *
     * @throws \Psr\Cache\InvalidArgumentException
     *
     * @return string[]
     */
    protected function retrieveTagVersions(array $tags): array
    {
        $tagIds = $this->getTagIdsMap($tags);
        \ksort($tagIds);

        $tagVersions = $generated = [];
        foreach ($this->tagPool->getItems(\array_keys($tagIds)) as $tagId => $version) {
            if ($version->isHit() && $tagVersion = $version->get()) {
                $tagVersions[$tagIds[$tagId]] = $tagVersion;
                continue;
            }
            $tagVersion = $this->generateTagVersion();
            $version->set($tagVersion);
            $this->tagPool->saveDeferred($version);
            $generated[$tagIds[$tagId]] = $tagVersion;
        }

        if (!$generated || $this->tagPool->commit()) {
            return $tagVersions + $generated;
        }

        return $tagVersions;
    }

    /**
     * Returns tag cache IDs to tag keys map.
     *
     * @param array $tags   Tag keys
     *
     * @return array        Tag IDs to tag keys map
     */
    protected function getTagIdsMap(array $tags): array
    {
        $tagIds = [];
        foreach ($tags as $tag) {
            $tagIds[$this->tagIdPrefix . $tag] = $tag;
        }

        return $tagIds;
    }

    /**
     * Generates unique string for robust tag versioning
     *
     * @return string
     */
    protected function generateTagVersion(): string
    {
        return \pack('L', \mt_rand()) . $this->instanceId;
    }

}
