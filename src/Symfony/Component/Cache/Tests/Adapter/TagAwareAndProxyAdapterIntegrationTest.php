<?php

namespace Symfony\Component\Cache\Tests\Adapter;

use PHPUnit\Framework\TestCase;
use Psr\Cache\CacheItemPoolInterface;
use Symfony\Component\Cache\Adapter\AbstractAdapter;
use Symfony\Component\Cache\Adapter\ArrayAdapter;
use Symfony\Component\Cache\Adapter\ProxyAdapter;
use Symfony\Component\Cache\Adapter\RedisAdapter;
use Symfony\Component\Cache\Adapter\TagAwareAdapter;
use Symfony\Component\Cache\Tests\Fixtures\ExternalAdapter;

class TagAwareAndProxyAdapterIntegrationTest extends TestCase
{
    /**
     * @dataProvider dataProvider
     */
    public function testIntegrationUsingProxiedAdapter(CacheItemPoolInterface $proxiedAdapter)
    {
        $cache = new TagAwareAdapter(new ProxyAdapter($proxiedAdapter));

        $item = $cache->getItem('foo');
        $item->tag(['tag1', 'tag2']);
        $item->set('bar');
        $cache->save($item);

        $this->assertSame('bar', $cache->getItem('foo')->get());

        $cache->invalidateTags(['tag2']);

        $this->assertFalse($cache->getItem('foo')->isHit());
    }

    /**
     * @dataProvider dataProvider
     */
    public function testIntegrationUsingProxiedAdapterForTagsPool(CacheItemPoolInterface $proxiedAdapter)
    {
        $arrayAdapter = new ArrayAdapter();
        $cache = new TagAwareAdapter($arrayAdapter, new ProxyAdapter($proxiedAdapter), 0.0);

        $item = $cache->getItem('foo');
        $item->expiresAfter(600);
        $item->tag(['baz']);
        $item->set('bar');
        $cache->save($item);

        $this->assertSame('bar', $cache->getItem('foo')->get());
        $this->assertTrue($cache->getItem('foo')->isHit());

        $cache->invalidateTags(['baz']);

        $this->assertFalse($cache->getItem('foo')->isHit());

        $cache->clear();
        $proxiedAdapter->clear();
    }

    public function dataProvider(): array
    {
        return [
            [new RedisAdapter(AbstractAdapter::createConnection('redis://'.getenv('REDIS_HOST'), ['lazy' => true]), str_replace('\\', '.', __CLASS__))],
            [new ArrayAdapter()],
            // also testing with a non-AdapterInterface implementation
            // because the ProxyAdapter behaves slightly different for those
            [new ExternalAdapter()],
        ];
    }
}
