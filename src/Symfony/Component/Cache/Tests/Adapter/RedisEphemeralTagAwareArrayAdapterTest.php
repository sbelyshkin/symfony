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

/**
 * @group integration
 */
class RedisEphemeralTagAwareArrayAdapterTest extends RedisArrayAdapterTest
{
    use TagAwareTestTrait;

    protected function setUp(): void
    {
        parent::setUp();
        $this->skippedTests['testPrune'] = 'Just proxies to underlying pools';
        $this->skippedTests['testTagItemExpiry'] = 'Testing expiration slows down the test suite';
    }

    public function createCachePool(int $defaultLifetime = 0, string $testMethod = null): CacheItemPoolInterface
    {
        if ('testClearWithPrefix' === $testMethod && \defined('Redis::SCAN_PREFIX')) {
            self::$redis->setOption(\Redis::OPT_SCAN, \Redis::SCAN_PREFIX);
        }

        $this->assertInstanceOf(\RedisArray::class, self::$redis);
        $adapter = new RedisEphemeralTagAwareAdapter(self::$redis, null, str_replace('\\', '.', __CLASS__), $defaultLifetime);

        return $adapter;
    }
}
