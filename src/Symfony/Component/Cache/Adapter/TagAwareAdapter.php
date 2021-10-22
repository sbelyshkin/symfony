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

use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Symfony\Component\Cache\PruneableInterface;
use Symfony\Component\Cache\ResettableInterface;
use Symfony\Contracts\Cache\TagAwareCacheInterface;

/**
 * @author Nicolas Grekas <p@tchwork.com>
 */
class TagAwareAdapter extends EphemeralTagAwareAdapter implements TagAwareAdapterInterface, TagAwareCacheInterface, PruneableInterface, ResettableInterface, LoggerAwareInterface
{
    use LoggerAwareTrait;

    public const TAGS_PREFIX = "\0tags\0";

    private $knownTagVersionsTtl;
    private $lastTagRetrievalTime;

    public function __construct(AdapterInterface $itemsPool, AdapterInterface $tagsPool = null, float $knownTagVersionsTtl = 0.15)
    {
        parent::__construct($itemsPool, $tagsPool);
        $this->knownTagVersionsTtl = $knownTagVersionsTtl;
    }

    /**
     * @return array
     */
    public function __sleep()
    {
        throw new \BadMethodCallException('Cannot serialize '.__CLASS__);
    }

    public function __wakeup()
    {
        throw new \BadMethodCallException('Cannot unserialize '.__CLASS__);
    }

    public function __destruct()
    {
        $this->commit();
    }

    /**
     * {@inheritDoc}
     */
    protected function clearLastRetrievedTagVersions(): void
    {
        if ($this->lastTagRetrievalTime && microtime(true) - $this->lastTagRetrievalTime > $this->knownTagVersionsTtl) {
            $this->lastTagRetrievalTime = null;
            parent::clearLastRetrievedTagVersions();
        }
    }

    /**
     * {@inheritDoc}
     */
    protected function retrieveTagVersions(array $tags): array
    {
        $this->lastTagRetrievalTime = microtime(true);

        return parent::retrieveTagVersions($tags);
    }
}
