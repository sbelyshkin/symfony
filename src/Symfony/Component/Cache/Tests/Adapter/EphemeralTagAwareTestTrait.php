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

use Symfony\Component\Cache\CacheItem;

/**
 * Test cases specific to EphemeralTagAware adapters.
 *
 * @method \Symfony\Component\Cache\Adapter\TagAwareAdapterInterface createCachePool(int $defaultLifetime = 0, string $testMethod = null) Must be implemented by TestCase
 */
trait EphemeralTagAwareTestTrait
{
    /**
     * @dataProvider provideDefaultLifetime
     */
    public function testOptimisticConcurrencyControl($defaultLifetime)
    {
        if (isset($this->skippedTests[__FUNCTION__])) {
            $this->markTestSkipped($this->skippedTests[__FUNCTION__]);
        }

        $cache = $this->createCachePool($defaultLifetime, __FUNCTION__);
        $anotherCacheClient = clone $cache;
        $value = mt_rand();

        // ---
        $cache->delete('foo');
        $this->assertSame($value, $cache->get('foo', function (CacheItem $item) use ($value, $anotherCacheClient) {
            $this->assertSame('foo', $item->getKey());
            $item->tag(['tag1', 'tag2']);
            $anotherCacheClient->invalidateTags(['tag1']); // emulate invalidation in between getItem() and save() inside get()

            return $value;
        }));
        $item = $cache->getItem('foo');
        $this->assertTrue($item->isHit(), "Invalidation happened before saving the item should not have impact on item's validity");
        $this->assertSame($value, $item->get(), "Invalidation happened before saving the item should not have impact on item's validity");

        // ---
        $cache->delete('foo');
        $this->assertSame($value.$value, $cache->get('foo', function (CacheItem $item) use ($value, $anotherCacheClient) {
            $this->assertSame('foo', $item->getKey());
            $item->tag(['tag1', 'tag2']);
            $anotherCacheClient->invalidateTags(['tag1']); // emulate invalidation in between getItem() and save() inside get()

            return function () use ($value) { return $value.$value; };
        }));
        $item = $cache->getItem('foo');
        $this->assertTrue($item->isHit(), "Invalidation happened before deferred computation should not have impact on item's validity");
        $this->assertSame($value.$value, $item->get(), "Invalidation happened before deferred computation should not have impact on item's validity");

        // ---
        $cache->delete('foo');
        $this->assertSame($value.$value.$value, $cache->get('foo', function (CacheItem $item) use ($value, $anotherCacheClient) {
            $this->assertSame('foo', $item->getKey());
            $item->tag(['tag1', 'tag2']);

            return function () use ($value, $anotherCacheClient) {
                $anotherCacheClient->invalidateTags(['tag2']); // emulate invalidation when computing value inside save()

                return $value.$value.$value;
            };
        }));
        $item = $cache->getItem('foo');
        $this->assertFalse($item->isHit(), 'The item must be invalidated if invalidation happens during deferred computation');
        $this->assertNull($item->get(), 'The item must be invalidated if invalidation happens during deferred computation');
    }

    public function provideDefaultLifetime()
    {
        return [[0], [1000]];
    }
}
