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
    public function testClearPrefix()
    {
        if (isset($this->skippedTests[__FUNCTION__])) {
            $this->markTestSkipped($this->skippedTests[__FUNCTION__]);
        }

        $cache = $this->createCachePool(0, __FUNCTION__);
        $cache->clear();

        // --- Start --- Test clearing by item prefix and a part of item key
        $item = $cache->getItem('foobar');
        $cache->save($item->set(1));
        $item = $cache->getItem('barfoo');
        $cache->save($item->set(2));
        $this->assertTrue($cache->hasItem('foobar'));
        $this->assertTrue($cache->hasItem('barfoo'));

        $cache->clear($cache::ITEM_PREFIX.'f');
        $this->assertFalse($cache->hasItem('foobar'));
        $this->assertTrue($cache->hasItem('barfoo'));
        // --- Start --- Test clearing by item prefix and a part of item key

        // --- Start --- Test clearing only by item prefix
        $item = $cache->getItem('foobar');
        $cache->save($item->set(10));
        $item = $cache->getItem('barfoo');
        $cache->save($item->set(20));
        $this->assertTrue($cache->hasItem('foobar'));
        $this->assertTrue($cache->hasItem('barfoo'));

        $cache->clear($cache::ITEM_PREFIX);
        $this->assertFalse($cache->hasItem('foobar'));
        $this->assertFalse($cache->hasItem('barfoo'));
        // --- End --- Test clearing only by item prefix

        // --- Start --- Test clearing without any prefix
        $item = $cache->getItem('foobar');
        $cache->save($item->set(100));
        $item = $cache->getItem('barfoo');
        $cache->save($item->set(200));
        $this->assertTrue($cache->hasItem('foobar'));
        $this->assertTrue($cache->hasItem('barfoo'));

        $cache->clear();
        $this->assertFalse($cache->hasItem('foobar'));
        $this->assertFalse($cache->hasItem('barfoo'));
        // --- End --- Test clearing without any prefix
    }

    public function testClearTagsByPrefix()
    {
        if (isset($this->skippedTests[__FUNCTION__])) {
            $this->markTestSkipped($this->skippedTests[__FUNCTION__]);
        }

        $cache = $this->createCachePool(0, __FUNCTION__);
        $cache->clear();

        // --- Start --- Test clearing by tag prefix and a part of tag name
        $item = $cache->getItem('foobar');
        $cache->save($item->tag('tagfoo')->set(1));
        $item = $cache->getItem('barfoo');
        $cache->save($item->tag('tagbar')->set(2));
        $this->assertTrue($this->hasTags($cache, 'tagfoo'));
        $this->assertTrue($this->hasTags($cache, 'tagbar'));
        $this->assertTrue($cache->hasItem('foobar'));
        $this->assertTrue($cache->hasItem('barfoo'));

        $cache->clear($cache::TAG_PREFIX.'tagf');
        $this->assertFalse($this->hasTags($cache, 'tagfoo'));
        $this->assertTrue($this->hasTags($cache, 'tagbar'));
        $this->assertFalse($cache->hasItem('foobar'));
        $this->assertTrue($cache->hasItem('barfoo'));
        // --- End --- Test clearing by tag prefix and a part of tag name

        // --- Start --- Test clearing only by tag prefix
        $item = $cache->getItem('foobar');
        $cache->save($item->tag('tagfoo')->set(10));
        $item = $cache->getItem('barfoo');
        $cache->save($item->tag('tagbar')->set(20));
        $this->assertTrue($this->hasTags($cache, 'tagfoo'));
        $this->assertTrue($this->hasTags($cache, 'tagbar'));
        $this->assertTrue($cache->hasItem('foobar'));
        $this->assertTrue($cache->hasItem('barfoo'));

        $cache->clear($cache::TAG_PREFIX);
        $this->assertFalse($this->hasTags($cache, 'tagfoo'));
        $this->assertFalse($this->hasTags($cache, 'tagbar'));
        $this->assertFalse($cache->hasItem('foobar'));
        $this->assertFalse($cache->hasItem('barfoo'));
        // --- End --- Test clearing only by tag prefix

        // --- Start --- Test clearing without any prefix
        $item = $cache->getItem('foobar');
        $cache->save($item->tag('tagfoo')->set(100));
        $item = $cache->getItem('barfoo');
        $cache->save($item->tag('tagbar')->set(200));
        $this->assertTrue($this->hasTags($cache, 'tagfoo'));
        $this->assertTrue($this->hasTags($cache, 'tagbar'));
        $this->assertTrue($cache->hasItem('foobar'));
        $this->assertTrue($cache->hasItem('barfoo'));

        $cache->clear();
        $this->assertFalse($this->hasTags($cache, 'tagfoo'));
        $this->assertFalse($this->hasTags($cache, 'tagbar'));
        $this->assertFalse($cache->hasItem('foobar'));
        $this->assertFalse($cache->hasItem('barfoo'));
        // --- End --- Test clearing without any prefix
    }

    public function testPassiveOptimisticLock()
    {
        if (isset($this->skippedTests[__FUNCTION__])) {
            $this->markTestSkipped($this->skippedTests[__FUNCTION__]);
        }

        $cache = $this->createCachePool(0, __FUNCTION__);
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
                $anotherCacheClient->invalidateTags(['tag2']); // emulate invalidation inside save()

                return $value.$value.$value;
            };
        }));
        $item = $cache->getItem('foo');
        $this->assertFalse($item->isHit(), 'The item must be invalidated if invalidation happens during deferred computation');
        $this->assertNull($item->get(), 'The item must be invalidated if invalidation happens during deferred computation');
    }
}
