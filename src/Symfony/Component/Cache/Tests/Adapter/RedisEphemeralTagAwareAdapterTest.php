<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Cache\Tests\Adapter;

use Psr\Cache\CacheItemPoolInterface;
use Symfony\Component\Cache\Adapter\RedisEphemeralTagAwareAdapter;
use Symfony\Component\Cache\CacheItem;
use Symfony\Component\Cache\Traits\RedisProxy;

/**
 * @group integration
 */
class RedisEphemeralTagAwareAdapterTest extends RedisAdapterTest
{
    use TagAwareTestTrait;
    use EphemeralTagAwareTestTrait;

    protected function setUp(): void
    {
        $this->skippedTests['testPrune'] = 'Just proxies to underlying pools';
        $this->skippedTests['testTagItemExpiry'] = 'Testing expiration slows down the test suite';
        parent::setUp();
    }

    public function createCachePool(int $defaultLifetime = 0, string $testMethod = null): CacheItemPoolInterface
    {
        if ('testClearWithPrefix' === $testMethod && \defined('Redis::SCAN_PREFIX')) {
            self::$redis->setOption(\Redis::OPT_SCAN, \Redis::SCAN_PREFIX);
        }

        $this->assertInstanceOf(RedisProxy::class, self::$redis);
        $adapter = new RedisEphemeralTagAwareAdapter(self::$redis, null, str_replace('\\', '.', __CLASS__), $defaultLifetime);

        return $adapter;
    }

    protected function hasTags(CacheItemPoolInterface $cache, string $tag): bool
    {
        $getTagIdsMapMethod = (new \ReflectionObject($cache))->getMethod('getTagIdsMap');
        $getTagIdsMapMethod->setAccessible(true);
        $tagIdsMap = $getTagIdsMapMethod->invoke($cache, [$tag]);

        foreach ($tagIdsMap as $tagId => $tagKey) {
            if (!self::$redis->exists($tagId)) {
                return false;
            }
        }

        return true;
    }
}
