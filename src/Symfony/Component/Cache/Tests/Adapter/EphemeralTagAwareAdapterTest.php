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
    protected function setUp(): void
    {
        $this->skippedTests['testPrune'] = 'Just proxies to item pool';
        $this->skippedTests['testTagItemExpiry'] = 'Testing expiration slows down the test suite';
        parent::setUp();
    }

    public function createCachePool(int $defaultLifetime = 0): CacheItemPoolInterface
    {
        return new EphemeralTagAwareAdapter(new FilesystemAdapter('', $defaultLifetime));
    }

    protected function hasTags(CacheItemPoolInterface $cache, string $tag): bool
    {
        $getTagIdsMapMethod = (new \ReflectionObject($cache))->getMethod('getTagIdsMap');
        $getTagIdsMapMethod->setAccessible(true);

        $tagPool = new FilesystemAdapter('');
        $getFileMethod = (new \ReflectionObject($tagPool))->getMethod('getFile');
        $getFileMethod->setAccessible(true);

        $tagIdsMap = $getTagIdsMapMethod->invoke($cache, [$tag]);
        foreach ($tagIdsMap as $tagId => $tagKey) {
            if ($this->isPruned($tagPool, $tagId)) {
                return false;
            }
        }

        return true;
    }

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

        // --- Start --- Test clearing only by common item prefix
        $item = $cache->getItem('foobar');
        $cache->save($item->set(10));
        $item = $cache->getItem('barfoo');
        $cache->save($item->set(20));
        $this->assertTrue($cache->hasItem('foobar'));
        $this->assertTrue($cache->hasItem('barfoo'));

        $cache->clear($cache::ITEM_PREFIX);
        $this->assertFalse($cache->hasItem('foobar'));
        $this->assertFalse($cache->hasItem('barfoo'));
        // --- End --- Test clearing only by common item prefix

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

    public function testClearByTagPrefix()
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

        // --- Start --- Test clearing only by common tag prefix
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
        // --- End --- Test clearing only by common tag prefix

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

}
