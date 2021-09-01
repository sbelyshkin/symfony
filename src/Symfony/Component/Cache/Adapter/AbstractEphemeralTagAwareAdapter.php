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
abstract class AbstractEphemeralTagAwareAdapter implements TagAwareAdapterInterface, TagAwareCacheInterface, PruneableInterface, ResettableInterface
{
    public const TAGS_PREFIX = "\0tags\0";

    use ContractsTrait;
    use ProxyTrait;

    /**
     * @var string
     */
    protected $tagIdPrefix = '';
    /**
     * @var CacheItem[]
     */
    private $deferred = [];
    /**
     * @var AdapterInterface
     */
    private $tagPool;
    private $createCacheItem;
    private $extractTags;
    private $computeValues;

    /**
     *
     * @param AdapterInterface $itemPool
     * @param AdapterInterface|null $tagPool
     */
    public function __construct(AdapterInterface $itemPool, AdapterInterface $tagPool = null)
    {
        $this->pool = $itemPool;
        $this->tagPool = $tagPool;
        if (!$tagPool) {
            $this->tagPool = $itemPool;
            $this->tagIdPrefix = static::TAGS_PREFIX;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function hasItem($key): bool
    {
        if (isset($this->deferred[$key])) {
            $this->commit();
        }

        if (!$this->pool->hasItem($key)) {
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
    }

    /**
     * {@inheritdoc}
     */
    public function getItems(array $keys = [])
    {
        if (!$keys) {
            return [];
        }
        if ($this->deferred) {
            $this->commit();
        }
        $invalidItemKeys = $items = $tags = [];
        foreach ($this->pool->getItems($keys) as $key => $item) {
            if (!$item->isHit()) {
                $items[$key] = $this->createCacheItem($key, null, false);
                continue;
            }
            $value = $item->get();
            // If the structure does not match what we expect, return an empty item and remove the value from pool
            if (!$this->isStructureValid($value)) {
                $items[$key] = $this->createCacheItem($key, null, false);
                $invalidItemKeys[] = $key;
                continue;
            }
            $tags += $this->extractTagVersionsFromValue($value);
            $items[$key] = $value;
        }

        // Try to benefit from bulk operations, if supported by tags pool
        $tagVersions = $this->getTagVersions(\array_keys($tags));

        foreach ($items as $key => $value) {
            if ($value instanceof CacheItem) {
                continue;
            }

            if ($this->containsInvalidTags($value, $tagVersions)) {
                $items[$key] = $this->createCacheItem($key, null, false);
                $invalidItemKeys[] = $key;
                continue;
            }

            $items[$key] = $this->createCacheItem($key, $value, true);
            // The item may be invalid due to its expiration and time discrepancy
            // but we won't delete it and let the cache collect the garbage itself
        }

        if ($invalidItemKeys) {
            $this->pool->deleteItems($invalidItemKeys);
        }

        return $items;
    }

    abstract protected function createCacheItem(string $key, ?array $value, bool $isHit): CacheItem;

    abstract protected function isStructureValid($value): bool;

    abstract protected function containsInvalidTags($value, array $tagVersions): bool;

    /**
     * {@inheritdoc}
     */
    public function clear(string $prefix = ''): bool
    {
        if ('' !== $prefix) {
            foreach ($this->deferred as $key => $item) {
                if (str_starts_with($key, $prefix)) {
                    unset($this->deferred[$key]);
                }
            }
        } else {
            $this->deferred = [];
        }

        if ($this->pool instanceof AdapterInterface) {
            return $this->pool->clear($prefix);
        }

        return $this->pool->clear();
    }

    /**
     * {@inheritdoc}
     */
    public function deleteItem($key): bool
    {
        return $this->pool->deleteItem($key);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteItems(array $keys): bool
    {
        return $this->pool->deleteItems($keys);
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
        return $this->tagPool->deleteItems($tags);
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
        $valuesByKey = $this->computeValues($this->deferred, $tagVersions);
        $allItemsAreValid = \count($this->deferred) === \count($valuesByKey);
        $this->deferred = [];
        foreach ($valuesByKey as $item) {
            $this->pool->saveDeferred($item);
        }

        return $this->pool->commit() && $allItemsAreValid;
    }

    abstract protected function extractTagVersionsFromValue($value): array;

    abstract protected function extractTagsFromItems(iterable $items): array;

    abstract protected function computeValues(iterable $items, array $tagVersions): array;

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
     * Gets tags from a tag storage or from its own "cache".
     *
     * May return only a part of requested tags or even none of them in case of technical issues.
     *
     * @param array $tags
     *
     * @throws \Psr\Cache\InvalidArgumentException
     *
     * @return string[]     Tag versions indexed by tag keys
     */
    abstract protected function getTagVersions(array $tags): array;

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
    abstract protected function generateTagVersion(): string;

}
