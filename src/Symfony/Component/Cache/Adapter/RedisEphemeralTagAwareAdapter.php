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

use Predis\Connection\Aggregate\ClusterInterface;
use Predis\Response\Status;
use Psr\Cache\CacheItemPoolInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Symfony\Component\Cache\CacheItem;
use Symfony\Component\Cache\Exception\InvalidArgumentException;
use Symfony\Component\Cache\Marshaller\MarshallerInterface;
use Symfony\Component\Cache\Traits\RedisClusterProxy;
use Symfony\Component\Cache\Traits\RedisProxy;
use Symfony\Component\Cache\Traits\RedisTrait;

/**
 * This Adapter leverages atomic operations, pipelining and other features of Redis.
 *
 * Note: in some distributed setups, when clients are allowed to read from replicas, consistency becomes eventual.
 * Due to asynchronous nature of replication there may be a notable lag during which readers may get false-positive
 * and false-negative results. The latter, in their turn, may result in extra writes in get-computeIfAbsent-put pattern.
 * It's not the case when all reads and writes go to a master node(s). Adapter doesn't change the source for reads,
 * it's your responsibility to configure redis client according to your needs.
 *
 * @requires Redis 2.6.12+
 *
 * @author Sergey Belyshkin <sbelyshkin@gmail.com>
 */
class RedisEphemeralTagAwareAdapter extends EphemeralTagAwareAdapter implements LoggerAwareInterface
{
    use LoggerAwareTrait;
    use RedisTrait;

    /**
     * While this Adapter can undoubtedly be used for dealing with persistent data, one of its aims is to provide
     * guaranteed tag-based invalidation to volatile storages which are commonly used for storing ephemeral data.
     * Usually, it's an LRU-caches but Redis has an options for its eviction policy. Along with 'allkeys-lru'
     * and 'allkeys-random', there are 'volatile-lru', 'volatile-random' and 'volatile-ttl' policies which instruct
     * Redis to evict only items with expire set when the memory limit is reached.
     *
     * In order to prevent the "Out Of Memory" state, Adapter uses non-zero default lifetime for tags and items.
     *
     * @see https://redis.io/topics/lru-cache
     *
     * After all, ephemeral literally means existing only one day :-)
     */
    private const DEFAULT_CACHE_TTL = 86400;

    /**
     * Lifetime for tags.
     *
     * This value is calculated by Adapter based on default lifetime for items. When default lifetime is 0,
     * lifetime for tags will also be 0, in other cases it's always greater than the given default lifetime and
     * not less than DEFAULT_CACHE_TTL. This rule makes it more likely that tags won't expire prior to items
     * and gives tags more chances over items to survive rounds of eviction when 'volatile-ttl' policy is chosen.
     *
     * @var int
     */
    protected $tagsLifetime = 0;
    /**
     * @var string
     */
    protected $namespace = '';
    /**
     * Strategy which creates Generator of SET NX instructions for specific redis client.
     *
     * @var Closure
     */
    private $setNxGenerator;

    /**
     * Hint: if you plan to store (all or some of) tagged items for about or more than DEFAULT_CACHE_TTL, then
     * it's good to pass planned maximum TTL as a default lifetime to give the Adapter a hint on TTL value for tags.
     *
     * @param \Redis|\RedisArray|\RedisCluster|\Predis\ClientInterface $redisClient     The redis client
     * @param CacheItemPoolInterface|null                              $itemPool        The cache pool for items
     * @param string                                                   $namespace       The namespace for tags (and for items if item pool is not provided)
     * @param int                                                      $defaultLifetime The default lifetime for items (expected maximal)
     * @param MarshallerInterface|null                                 $marshaller      The marshaller for items
     */
    public function __construct($redisClient, CacheItemPoolInterface $itemPool = null, string $namespace = '', int $defaultLifetime = self::DEFAULT_CACHE_TTL, MarshallerInterface $marshaller = null)
    {
        $this->init($redisClient, $namespace, $defaultLifetime);
        if (null === $itemPool) {
            $itemPool = new RedisAdapter($redisClient, $namespace, $defaultLifetime, $marshaller);
        }
        parent::__construct($itemPool, $itemPool);
    }

    /**
     * {@inheritdoc}
     */
    public function invalidateTags(array $tags): bool
    {
        parent::clearLastRetrievedTagVersions();
        $tagIdsMap = $this->getTagIdsMap($tags);

        return $this->doDelete(array_keys($tagIdsMap));
    }

    public function clear(string $prefix = ''): bool
    {
        $this->doClear($this->namespace.$prefix);

        return parent::clear($prefix);
    }

    /**
     * {@inheritdoc}
     */
    protected function retrieveTagVersions(array $tags): array
    {
        $tagIds = $this->getTagIdsMap($tags);
        \ksort($tagIds);

        $results = $this->fetchRaw(\array_keys($tagIds));

        $tagVersions = [];
        foreach ($results as $id => $tagVersion) {
            $tagVersions[$tagIds[$id]] = $tagVersion;
        }

        if (!$tagIds = \array_diff_key($tagIds, $results)) {
            // If tags have TTL, update it from time to time
            if ($this->tagsLifetime && \rand(0, static::DEFAULT_CACHE_TTL) < 60) {
                // Since DEFAULT_CACHE_TTL is the minimal lifetime for tags, this formula means that
                // if the tags are read on average more than 1 time per 60 seconds, then they have a good chance
                // of infinite prolongation of their lives (not counting invalidations and evictions).
                $this->refreshTagIds(\array_keys($results));
            }

            return $tagVersions;
        }

        $newTagVersion = $this->generateTagVersion();

        $results = $this->pipeline(($this->setNxGenerator)($tagIds, $newTagVersion, $this->tagsLifetime));

        foreach ($results as $id => $result) {
            // SET NX results
            if (true !== $result && (!$result instanceof Status || Status::get('OK') !== $result)) {
                continue;
            }
            // Return only known tag versions
            $tagVersions[$tagIds[$id]] = $newTagVersion;
        }

        return $tagVersions;
    }

    /**
     * {@inheritdoc}
     */
    protected function getTagIdsMap(array $tags): array
    {
        $fullPrefix = $this->namespace.static::TAG_PREFIX;
        $tagIds = [];
        foreach ($tags as $tag) {
            $tagIds[$fullPrefix.$tag] = $tag;
        }

        return $tagIds;
    }

    /**
     * Refresh TTL of the given tags.
     */
    private function refreshTagIds(array $tagIds): void
    {
        if (!$this->tagsLifetime || !$tagIds) {
            return;
        }

        $startTime = \microtime(true);

        $results = $this->pipeline(static function () use ($tagIds) {
            foreach ($tagIds as $id) {
                // Available since 2.6.0
                yield 'pttl' => [$id];
            }
        });
        $tagTtls = \array_combine($tagIds, \iterator_to_array($results));

        $ttl = $this->tagsLifetime;
        $results = $this->pipeline(static function () use ($tagIds, $ttl) {
            foreach ($tagIds as $id) {
                yield 'expire' => [$id, $ttl];
            }
        });
        $gone = \array_keys(\array_diff_key($tagTtls, \array_filter(\array_combine($tagIds, \iterator_to_array($results)))));

        $time = \sprintf('%.6f',1000 * (\microtime(true) - $startTime));
        $message = 'Some tags were lucky to get their TTL refreshed! Their names and TTLs in ms are: "{tags}" but "{gone}" have suddenly gone. New TTL is {ttl}s. Update took {utime}ms.';
        CacheItem::log($this->logger, $message, ['tags' => \json_encode($tagTtls), 'gone' => \json_encode($gone), 'ttl' => $ttl, 'utime' => $time, 'cache-adapter' => \get_debug_type($this)]);
    }

    /**
     * Fetches raw strings from Redis.
     *
     * @param string[] $ids
     *
     * @return string[] Successfully fetched values indexed by ids
     */
    private function fetchRaw(array $ids): array
    {
        if ($this->redis instanceof \Predis\Client && $this->redis->getConnection() instanceof ClusterInterface) {
            $values = $this->pipeline(function () use ($ids) {
                foreach ($ids as $id) {
                    yield 'get' => [$id];
                }
            });
            $values = \iterator_to_array($values);
        } else {
            $values = \array_combine($ids, $this->redis->mget($ids));
        }

        return \array_filter($values, static function($v) { return \is_string($v); });
    }

    /**
     * @param \Redis|\RedisArray|\RedisCluster|\Predis\ClientInterface $redisClient
     * @param string                                                   $namespace
     * @param int                                                      $defaultLifetime
     */
    private function init($redisClient, string $namespace, int $defaultLifetime)
    {
        if (\preg_match('#[^-+_.A-Za-z0-9]#', $namespace, $match)) {
            throw new InvalidArgumentException(sprintf('RedisAdapter namespace contains "%s" but only characters in [-+_.A-Za-z0-9] are allowed.', $match[0]));
        }

        if (!$redisClient instanceof \Redis && !$redisClient instanceof \RedisArray && !$redisClient instanceof \RedisCluster && !$redisClient instanceof \Predis\ClientInterface && !$redisClient instanceof RedisProxy && !$redisClient instanceof RedisClusterProxy) {
            throw new InvalidArgumentException(sprintf('"%s()" expects parameter 1 to be Redis, RedisArray, RedisCluster or Predis\ClientInterface, "%s" given.', __METHOD__, get_debug_type($redisClient)));
        }

        $this->tagsLifetime = 0 < $defaultLifetime ? \max(static::DEFAULT_CACHE_TTL / 3, $defaultLifetime) * 3 : 0;
        $this->namespace = '' === $namespace ? '' : $namespace.':';

        if ($this->tagsLifetime) {
            // Advanced options of SET operation have to be passed in a form specific to the redis client
            if ($redisClient instanceof \Predis\ClientInterface) {
                $this->setNxGenerator = function ($tagIds, $value, $ttl) {
                    return static function () use ($tagIds, $value, $ttl) {
                        foreach ($tagIds as $id => $tag) {
                            // Available since 2.6.12
                            yield 'set' => [$id, $value, 'EX', $ttl, 'NX'];
                        }
                    };
                };
            } else {
                // phpredis family
                $this->setNxGenerator = function ($tagIds, $value, $ttl) {
                    return static function () use ($tagIds, $value, $ttl) {
                        foreach ($tagIds as $id => $tag) {
                            // Available since 2.6.12
                            yield 'set' => [$id, $value, ['EX' => $ttl, 'NX']];
                        }
                    };
                };
            }
        } else {
            $this->setNxGenerator = function ($tagIds, $value, $ttl) {
                return static function () use ($tagIds, $value) {
                    foreach ($tagIds as $id => $tag) {
                        // Available since 1.0.0.
                        yield 'setNx' => [$id, $value];
                    }
                };
            };
        }

        if ($redisClient instanceof \Predis\ClientInterface && $redisClient->getOptions()->exceptions) {
            $options = clone $redisClient->getOptions();
            \Closure::bind(function () { $this->options['exceptions'] = false; }, $options, $options)();
            $redisClient = new $redisClient($redisClient->getConnection(), $options);
        }

        $this->redis = $redisClient;
    }
}
