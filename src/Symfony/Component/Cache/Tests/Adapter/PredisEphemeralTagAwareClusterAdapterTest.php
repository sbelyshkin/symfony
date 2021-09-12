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
class PredisEphemeralTagAwareClusterAdapterTest extends PredisClusterAdapterTest
{
    use TagAwareTestTrait;
    use EphemeralTagAwareTestTrait;

    protected function setUp(): void
    {
        parent::setUp();
        $this->skippedTests['testPrune'] = 'Just proxies to underlying pools';
        $this->skippedTests['testTagItemExpiry'] = 'Testing expiration slows down the test suite';
    }

    public function createCachePool(int $defaultLifetime = 0, string $testMethod = null): CacheItemPoolInterface
    {
        $this->assertInstanceOf(\Predis\Client::class, self::$redis);
        $adapter = new RedisEphemeralTagAwareAdapter(self::$redis, null, str_replace('\\', '.', __CLASS__), $defaultLifetime);

        return $adapter;
    }
}
