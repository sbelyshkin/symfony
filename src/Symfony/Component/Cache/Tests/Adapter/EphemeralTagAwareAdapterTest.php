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
use Symfony\Component\Cache\Adapter\EphemeralTagAwareAdapter;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;

/**
 * @group integration
 */
class EphemeralTagAwareAdapterTest extends FilesystemTagAwareAdapterTest
{
    use EphemeralTagAwareTestTrait;

    protected function setUp(): void
    {
        $this->skippedTests['testPrune'] = 'Just proxies to underlying pools';
        $this->skippedTests['testTagItemExpiry'] = 'Testing expiration slows down the test suite';
        parent::setUp();
    }

    public function createCachePool(int $defaultLifetime = 0, string $testMethod = null): CacheItemPoolInterface
    {
        return new EphemeralTagAwareAdapter(new FilesystemAdapter('', $defaultLifetime));
    }

    protected function hasTags(CacheItemPoolInterface $cache, string $tag): bool
    {
        $getTagIdsMapMethod = (new \ReflectionObject($cache))->getMethod('getTagIdsMap');
        $getTagIdsMapMethod->setAccessible(true);

        $tagPool = new FilesystemAdapter('');

        $tagIdsMap = $getTagIdsMapMethod->invoke($cache, [$tag]);
        foreach ($tagIdsMap as $tagId => $tagKey) {
            if ($this->isPruned($tagPool, $tagId)) {
                return false;
            }
        }

        return true;
    }
}
