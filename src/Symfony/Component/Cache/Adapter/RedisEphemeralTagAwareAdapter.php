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

use Predis\Response\Status;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Symfony\Component\Cache\CacheItem;
use Symfony\Component\Cache\Exception\InvalidArgumentException;
use Symfony\Component\Cache\Marshaller\DefaultMarshaller;
use Symfony\Component\Cache\Marshaller\MarshallerInterface;
use Symfony\Component\Cache\Traits\RedisClusterProxy;
use Symfony\Component\Cache\Traits\RedisProxy;
use Symfony\Component\Cache\Traits\RedisTrait;

/**
 * This Adapter is designed as a safe storage with tag-based invalidation.
 * The safety implies ability to invalidate any item by the tag it was saved with.
 * This ability does not affected by peak loads and out-of-memory state.
 * 2)
 * If set to a non-zero positive value, considered as a default lifetime for items and
 * used for calculation of minimal sufficient lifetime for tags to make them eventually evictable,
 * as well as items but less frequently.
 *
 * @requires Redis 2.6.12+
 *
 * @author Sergey Belyshkin <sbelyshkin@gmail.com>
 */
class RedisEphemeralTagAwareAdapter extends EphemeralTagAwareAdapter implements LoggerAwareInterface
{
    public const NS_SEPARATOR = ':';
    /**
     * Although it's safe to store items with no expiry set in a true LRU cache, with Redis you should consider its eviction policy.
     * To make items eventually evictable, even with 'noeviction' policy in effect, the expiration time must be set.
     * As a reminder of that this Adapter uses non-zero default lifetime.
     *
     * @link https://redis.io/topics/lru-cache
     */
    private const DEFAULT_CACHE_TTL = 86400;

    use LoggerAwareTrait;
    use RedisTrait;

    /**
     * Lifetime for tags
     *
     * This value is calculated by Adapter based on default life time for items. When the default lifetime is 0,
     * lifetime for tags is also 0, in other cases it's always greater than the given default lifetime and
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
     * Hint: if 'noeviction' or 'volatile-ttl' policy is in effect and you plan to store (some of) tagged items
     *      for about or more than DEFAULT_CACHE_TTL, then it's good to pass planned maximum TTL as a default lifetime
     *      to give the Adapter a hint on TTL value for tags.
     *
     * @param \Redis|\RedisArray|\RedisCluster|\Predis\ClientInterface  $redisClient     The redis client
     * @param AdapterInterface|null                                     $itemPool        The pool for items
     * @param string                                                    $namespace       The namespace for tags (and for items if item pool is not given)
     * @param int                                                       $defaultLifetime The default lifetime for items (expected maximal)
     * @param MarshallerInterface|null                                  $marshaller      THe marshaller for tags and items
     */
    public function __construct($redisClient, AdapterInterface $itemPool = null, string $namespace = '', int $defaultLifetime = self::DEFAULT_CACHE_TTL, MarshallerInterface $marshaller = null)
    {
        $this->init($redisClient, $namespace, $defaultLifetime, $marshaller);
        if (null === $itemPool) {
            $itemPool = new RedisAdapter($redisClient, $namespace, $defaultLifetime, $marshaller);
            $this->tagIdPrefix = static::TAGS_PREFIX;
        }
        parent::__construct($itemPool, $itemPool);
    }

    /**
     * {@inheritdoc}
     */
    public function invalidateTags(array $tags): bool
    {
        parent::clearLastRetrievedTagVersions();

        return $this->doDelete($tags);
    }

    /**
     * {@inheritdoc}
     */
    protected function retrieveTagVersions(array $tags): array
    {
        $tagIds = $this->getTagIdsMap($tags);
        \ksort($tagIds);

        $results = $this->doFetch(\array_keys($tagIds));

        $tagVersions = [];
        foreach ($results as $id => $tagVersion) {
            $tagVersions[$tagIds[$id]] = $tagVersion;
        }

        if (!$tagIds = \array_diff_key($tagIds, $results)) {
            // If tags have TTL, update it from time to time
            if (1||$this->tagsLifetime && \rand(0, static::DEFAULT_CACHE_TTL) < 60) {
                // Since DEFAULT_CACHE_TTL is the minimal lifetime for tags, this formula means that
                // if the tags are read on average more than 1 time per 60 seconds, then they have a good chance
                // of infinite prolongation of their lives (omitting invalidations and evictions).
                $this->refreshTagIds(\array_keys($results));
            }

            return $tagVersions;
        }

        $tagVersion = $this->generateTagVersion();
        // Serialize single string
        if (!$serialized = $this->marshaller->marshall([$tagVersion], $failed)) {
            // Return only existing tag versions
            return $tagVersions;
        }

        $serialized = $serialized[0];
        if ($this->tagsLifetime) {
            $ttl = $this->tagsLifetime;
            $setNxGenerator = static function () use ($tagIds, $serialized, $ttl) {
                foreach ($tagIds as $id => $tag) {
                    // Supported by Redis since v2.6.12
                    // @todo Perhaps we'll benefit from advanced SET NX GET on Redis 7.0+
                    yield 'set' => [$id, $serialized, 'EX', $ttl, 'NX'];
                }
            };
        } else {
            $setNxGenerator = static function () use ($tagIds, $serialized) {
                foreach ($tagIds as $id => $tag) {
                    // Supported by Redis since v2.6.12
                    yield 'set' => [$id, $serialized,'NX'];
                }
            };
        }

        $results = $this->pipeline($setNxGenerator);
        foreach ($results as $id => $result) {
            // set NX results
            if (true !== $result && (!$result instanceof Status || Status::get('OK') !== $result)) {
                continue;
            }
            $tagVersions[$tagIds[$id]] = $tagVersion;
        }

        return $tagVersions;
    }

    /**
     * {@inheritdoc}
     */
    protected function getTagIdsMap(array $tags): array
    {
        $tagIds = [];
        foreach ($tags as $tag) {
            $tagIds[$this->namespace . $this->tagIdPrefix . $tag] = $tag;
        }

        return $tagIds;
    }

    /**
     * @param \Redis|\RedisArray|\RedisCluster|\Predis\ClientInterface  $redisClient
     * @param string                                                    $namespace
     * @param int                                                       $defaultLifetime
     * @param MarshallerInterface|null                                  $marshaller
     */
    private function init($redisClient, string $namespace, int $defaultLifetime, ?MarshallerInterface $marshaller)
    {
        if (preg_match('#[^-+_.A-Za-z0-9]#', $namespace, $match)) {
            throw new InvalidArgumentException(sprintf('RedisAdapter namespace contains "%s" but only characters in [-+_.A-Za-z0-9] are allowed.', $match[0]));
        }

        if (!$redisClient instanceof \Redis && !$redisClient instanceof \RedisArray && !$redisClient instanceof \RedisCluster && !$redisClient instanceof \Predis\ClientInterface && !$redisClient instanceof RedisProxy && !$redisClient instanceof RedisClusterProxy) {
            throw new InvalidArgumentException(sprintf('"%s()" expects parameter 1 to be Redis, RedisArray, RedisCluster or Predis\ClientInterface, "%s" given.', __METHOD__, get_debug_type($redisClient)));
        }

        if ($redisClient instanceof \Predis\ClientInterface && $redisClient->getOptions()->exceptions) {
            $options = clone $redisClient->getOptions();
            \Closure::bind(function () { $this->options['exceptions'] = false; }, $options, $options)();
            $redisClient = new $redisClient($redisClient->getConnection(), $options);
        }

        $this->tagsLifetime = 0 < $defaultLifetime ? \max(static::DEFAULT_CACHE_TTL / 3, $defaultLifetime) * 3 : 0;
        $this->namespace = '' === $namespace ? '' : $namespace . static::NS_SEPARATOR;
        $this->redis = $redisClient;
        $this->marshaller = $marshaller ?? new DefaultMarshaller();
    }

    /**
     * Refresh TTL of the given tags.
     *
     * @todo Perhaps we'll benefit from advanced EXPIRE GT on Redis 7.0+
     * @link https://redis.io/commands/expire
     *
     * @param array $tagIds
     */
    private function refreshTagIds(array $tagIds): void
    {
        if (!$this->tagsLifetime || !$tagIds) {
            return;
        }

        $startTime = microtime(true);

        $results = $this->pipeline(static function () use ($tagIds) {
            foreach ($tagIds as $id) {
                yield 'pttl' => [$id];
            }
        });
        $tagTtls = array_combine($tagIds, iterator_to_array($results));

        $ttl = $this->tagsLifetime;
        $results = $this->pipeline(static function () use ($tagIds, $ttl) {
            foreach ($tagIds as $id) {
                yield 'expire' => [$id, $ttl];
            }
        });
        $gone = array_keys(array_diff_key($tagTtls, array_filter(array_combine($tagIds, iterator_to_array($results)))));

        $time = sprintf('%.6f',1000 * (microtime(true) - $startTime));
        $message = 'Some tags were lucky to get their TTL refreshed! Their names and TTLs in ms are: "{tags}" but "{gone}" have suddenly gone. New TTL is {ttl}s. Update took {utime}ms.';
        CacheItem::log($this->logger, $message, ['tags' => json_encode($tagTtls), 'gone' => json_encode($gone), 'ttl' => $ttl, 'utime' => $time, 'cache-adapter' => get_debug_type($this)]);
    }

}
