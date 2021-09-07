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
use Symfony\Component\Cache\PruneableInterface;
use Symfony\Component\Cache\ResettableInterface;
use Symfony\Component\Cache\Traits\ContractsTrait;
use Symfony\Contracts\Cache\ItemInterface;
use Symfony\Contracts\Cache\TagAwareCacheInterface;

/**
 * Abstract class for EphemeralTagAware adapters family.
 *
 * This family of adapters is intended to provide guaranteed tag-base invalidation for volatile cache storages
 * as well as for those which provide persistence.
 *
 * Tags are a separate variables stored in a cache, their values are tag versions which are changed
 * after every invalidation. Tag versions are an integral part of item's value, so tagging an item
 * (setting or changing related set of tags) is only possible as a part of storing that item.
 * If requested tags' versions cannot be obtained for any reason, Adapter rejects storing the item.
 * The value of an item is valid until tag versions it was saved with (any of them) are changed.
 *
 * Although invalidation can be fulfilled in various ways, simple deletion of a tag variable is preferred.
 * Deletion is atomic operation and, in comparison to incrementing or setting a new value, is safe in situations
 * when the storage is out of space. This gives an additional guarantee of not getting an invalidated items from cache.
 *
 * @author Sergey Belyshkin <sbelyshkin@gmail.com>
 */
abstract class AbstractEphemeralTagAwareAdapter implements TagAwareAdapterInterface, TagAwareCacheInterface, PruneableInterface, ResettableInterface
{
    public const ITEM_PREFIX = '$';
    public const TAGS_PREFIX = "#";

    use ContractsTrait;

    /**
     * @var CacheItemPoolInterface
     */
    private $pool;
    /**
     * @var CacheItemPoolInterface
     */
    private $tagPool;
    /**
     * @var CacheItem[]
     */
    private $deferred = [];
    /**
     * @var \Closure
     */
    private $createCacheItem;
    /**
     * @var \Closure
     */
    private $extractTagsFromItems;

    /**
     *
     * @param CacheItemPoolInterface $itemPool
     * @param CacheItemPoolInterface|null $tagPool
     */
    public function __construct(CacheItemPoolInterface $itemPool, CacheItemPoolInterface $tagPool = null)
    {
        $this->pool = $itemPool;
        $this->tagPool = $tagPool;
        if (!$tagPool) {
            $this->tagPool = $itemPool;
        }

        $this->createCacheItem = \Closure::bind(
            static function ($key, $isHit, $value, $meta) {
                $item = new CacheItem();
                $item->key = $key;
                $item->isTaggable = true;
                $item->isHit = $isHit;
                $item->value = $value;
                $item->metadata = $meta;

                return $item;
            },
            null,
            CacheItem::class
        );

        $this->extractTagsFromItems = \Closure::bind(
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

    }

    /**
     * {@inheritdoc}
     */
    public function prune()
    {
        $this->pool instanceof PruneableInterface && $this->pool->prune();
        $this->tagPool instanceof PruneableInterface && $this->tagPool->prune();
    }

    /**
     * {@inheritdoc}
     */
    public function reset()
    {
        $this->commit();
        $this->pool instanceof ResettableInterface && $this->pool->reset();
        $this->tagPool instanceof ResettableInterface && $this->tagPool->reset();
    }

    /**
     * {@inheritdoc}
     */
    public function hasItem($key): bool
    {
        if (isset($this->deferred[$key])) {
            $this->commit();
        }

        if (!$this->pool->hasItem($this->getPrefixedKey($key))) {
            return false;
        }

        $item = $this->getItem($key);

        return $item->isHit();
    }

    /**
     * {@inheritdoc}
     */
    public function getItem($key): CacheItem
    {
        foreach ($this->getItems([$key]) as $item) {
            return $item;
        }

        return $this->createCacheItem($key);
    }

    /**
     * {@inheritdoc}
     */
    public function getItems(array $keys = [])
    {
        if (!$keys) {
            return [];
        }

        if ($this->deferred && \array_intersect_key($this->deferred, \array_flip($keys))) {
            $this->commit();
        }

        $prefixedKeys = \array_map([$this, 'getPrefixedKey'], $keys);
        $itemIdsMap = \array_combine($prefixedKeys, $keys);
        $expiredItemKeys = $invalidItemKeys = $items = $tags = [];
        foreach ($this->pool->getItems($prefixedKeys) as $itemId => $item) {
            $key = $itemIdsMap[$itemId];
            if (!$item->isHit()) {
                $items[$key] = null;
                continue;
            }

            $itemData = $this->unpackItem($item);

            if (!$itemData) {
                $invalidItemKeys[] = $key;
                $items[$key] = null;
                continue;
            }

            // Even if cache storage tracks item's TTL, the item may be expired because of time discrepancy
            if (isset($itemData['meta'][CacheItem::METADATA_EXPIRY]) && $itemData['meta'][CacheItem::METADATA_EXPIRY] < \microtime(true)) {
                $expiredItemKeys[] = $key;
                $items[$key] = null;
                continue;
            }

            $tags += $itemData['tagVersions'];
            $items[$key] = $itemData;
        }

        $this->evictExpiredItems($expiredItemKeys);
        $prefixedKeys = $itemIdsMap = $expiredItemKeys = null;

        $tagVersions = $this->getTagVersions(\array_keys($tags));

        foreach ($items as $key => $itemData) {
            if (null === $itemData) {
                $items[$key] = $this->createCacheItem($key);
                continue;
            }

            if (!$this->isTagVersionsValid($itemData['tagVersions'], $tagVersions)) {
                $invalidItemKeys[] = $key;
                $items[$key] = $this->createCacheItem($key);
                continue;
            }

            $items[$key] = $this->createCacheItem($key, true, $itemData['value'], $itemData['meta']);
        }

        $this->evictInvalidItems($invalidItemKeys);

        return $items;
    }


    /**
     * Evicts items from pool.
     *
     * Called when expired items retrieved from the pool.
     *
     * @param array $expiredItemKeys
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    protected function evictExpiredItems(array $expiredItemKeys)
    {
        if ($expiredItemKeys) {
            $this->deleteItems($expiredItemKeys);
        }
    }
    /**
     * Evicts items from pool.
     *
     * Called when items with invalid structure or with invalidated tag versions retrieved from pool.
     *
     * @param array $invalidItemKeys
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    protected function evictInvalidItems(array $invalidItemKeys)
    {
        if ($invalidItemKeys) {
            $this->deleteItems($invalidItemKeys);
        }
    }

    protected function createCacheItem(string $key, bool $isHit = false, $value = null, array $metadata = []): CacheItem
    {
        return ($this->createCacheItem)($key, $isHit, $value, $metadata);
    }

    /**
     * {@inheritdoc}
     */
    public function clear(string $prefix = ''): bool
    {
        if ($this->pool instanceof AdapterInterface) {
            $isPoolCleared = $this->pool->clear($prefix);
        } else {
            $isPoolCleared = $this->pool->clear();
        }

        if ($this->tagPool instanceof AdapterInterface) {
            $isTagPoolCleared = $this->tagPool->clear($prefix);
        } else {
            $isTagPoolCleared = $this->tagPool->clear();
        }

        if ('' !== $prefix) {
            foreach ($this->deferred as $key => $item) {
                if (str_starts_with($key, $prefix)) {
                    unset($this->deferred[$key]);
                }
            }
        } else {
            $this->deferred = [];
        }

        return $isPoolCleared && $isTagPoolCleared;
    }

    /**
     * {@inheritdoc}
     */
    public function deleteItem($key): bool
    {
        $key = $this->getPrefixedKey($key);

        return $this->pool->deleteItem($key);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteItems(array $keys): bool
    {
        $prefixedKeys = \array_map([$this, 'getPrefixedKey'], $keys);

        return $this->pool->deleteItems($prefixedKeys);
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    public function save(CacheItemInterface $item): bool
    {
        return $this->saveDeferred($item) && $this->commit();
    }

    /**
     * {@inheritdoc}
     */
    public function saveDeferred(CacheItemInterface $item): bool
    {
        if (!$item instanceof CacheItem) {
            return false;
        }
        $this->deferred[$item->getKey()] = $item;

        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function invalidateTags(array $tags): bool
    {
        $tagIdsMap = $this->getTagIdsMap($tags);

        return $this->tagPool->deleteItems(\array_keys($tagIdsMap));
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    public function commit(): bool
    {
        if (!$this->deferred) {
            return true;
        }
        $uniqueTags = $this->extractTagsFromItems($this->deferred);
        $tagVersions = $this->getTagVersions($uniqueTags);
        $packedItems = $this->packItems($this->deferred, $tagVersions);
        $allItemsArePacked = \count($this->deferred) === \count($packedItems);
        $this->deferred = [];
        foreach ($packedItems as $item) {
            $this->pool->saveDeferred($item);
        }

        return $this->pool->commit() && $allItemsArePacked;
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

    /**
     * @param CacheItem[]|iterable $items
     * @return array
     */
    protected function extractTagsFromItems(iterable $items): array
    {
        return ($this->extractTagsFromItems)($items);
    }

    /**
     * Computes item values and packs them with item tags' versions
     * into a new items for storing in the item pool.
     *
     * @param ItemInterface[]|iterable $items
     * @param array $tagVersions
     *
     * @return CacheItemInterface[]  Items with all packed data as a value
     */
    abstract protected function packItems(iterable $items, array $tagVersions): array;

    /**
     * Unpacks an item retrieved from the item pool.
     *
     * @param CacheItemInterface $item
     *
     * @return array{value: mixed, tagVersions: array, meta: array}
     */
    abstract protected function unpackItem(CacheItemInterface $item): array;

    /**
     * Gets tags from a tag storage or from its own "cache".
     *
     * May return only a part of requested tags or even none of them if for some reason they cannot be read or created.
     *
     * @param array $tags
     *
     * @throws \Psr\Cache\InvalidArgumentException
     *
     * @return string[]     Tag versions indexed by tag keys
     */
    protected function getTagVersions(array $tags): array
    {
        if (!$tags) {
            return [];
        }

        return $this->retrieveTagVersions($tags);
    }

    /**
     * Loads tags from or creates new ones in a tag pool.
     *
     * May return only a part of requested tags or even none of them if for some reason they cannot be read or created.
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

        // Use of one stamp for many tags is good; when they are stored together, igbinary has 'compact_string' option for them
        $newTagVersion = $this->generateTagVersion();
        $tagVersions = $generated = [];
        foreach ($this->tagPool->getItems(\array_keys($tagIds)) as $tagId => $version) {
            if ($version->isHit() && is_scalar($tagVersion = $version->get())) {
                $tagVersions[$tagIds[$tagId]] = $tagVersion;
                continue;
            }
            $version->set($newTagVersion);
            $this->tagPool->saveDeferred($version);
            $generated[$tagIds[$tagId]] = $newTagVersion;
        }

        if (!$generated || $this->tagPool->commit()) {
            return $tagVersions + $generated;
        }

        return $tagVersions;
    }

    /**
     * @param $key
     *
     * @return string
     */
    protected function getPrefixedKey($key): string
    {
        return static::ITEM_PREFIX.$key;
    }

    /**
     * Returns tag IDs to tag keys map.
     *
     * @param string[] $tags    Tag keys
     *
     * @return array            Tag IDs to tag keys map
     */
    protected function getTagIdsMap(array $tags): array
    {
        $tagIds = [];
        foreach ($tags as $tag) {
            $tagIds[static::TAGS_PREFIX . $tag] = $tag;
        }

        return $tagIds;
    }

    /**
     * Generates unique string for robust tag versioning
     *
     * @return string
     */
    abstract protected function generateTagVersion(): string;

    /**
     * Checks a set of tag versions against available current ones
     *
     * @param array $itemTagVersions    Item's tag versions
     * @param array $tagVersions        Current tag version to test against
     *
     * @return bool True if all item's tag versions match current ones
     */
    private function isTagVersionsValid(array $itemTagVersions, array $tagVersions): bool
    {
        ksort($itemTagVersions);
        ksort($tagVersions);

        return $itemTagVersions === \array_intersect_key($tagVersions, $itemTagVersions);
    }

}
