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

use Doctrine\ORM\Mapping as ORM;
use Psr\Cache\CacheItemInterface;
use Psr\Cache\CacheItemPoolInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerAwareTrait;
use Symfony\Component\Cache\CacheItem;
use Symfony\Component\Cache\PruneableInterface;
use Symfony\Component\Cache\ResettableInterface;
use Symfony\Component\Cache\Traits\ContractsTrait;
use Symfony\Component\Cache\Traits\ProxyTrait;
use Symfony\Contracts\Cache\CacheInterface;

/**
 * Proxies calls to AdapterInterface::class or any other PSR-6 compliant adapter
 * retrying to get individual items if a getItem() call fails. Other calls, including getItems(),
 * are just forwarded to underlying adapter with no retries.
 *
 * Calls to methods of CacheInterface::class are forwarded to underlying adapter if it implements the interface,
 * and if the ContractsTrait is used then the locks or other wrappers around callback function are disabled.
 * Otherwise, own implementation of CacheInterface is used to leverage retries.
 *
 * Allowed number of retries are distributed over the timeout interval according to chosen distribution strategy.
 *
 * @author Sergey Belyshkin <sbelyshkin@gmail.com>
 */
class RetryProxyAdapter implements AdapterInterface, CacheInterface, LoggerAwareInterface, PruneableInterface, ResettableInterface
{
    public const STRATEGY_DEFAULT = self::STRATEGY_FLAT_DISTRIBUTION_GEOMETRIC_INTERVALS;
    public const STRATEGY_NORMAL_DISTRIBUTION_RANDOM_INTERVALS = 1;
    public const STRATEGY_FLAT_DISTRIBUTION_EVEN_INTERVALS = 2;
    public const STRATEGY_FLAT_DISTRIBUTION_GEOMETRIC_INTERVALS = 3;
    public const STRATEGY_DELTA_DISTRIBUTION_EVEN_INTERVALS = 4;
    public const STRATEGY_FLAT_DISTRIBUTION_RANDOM_INTERVALS = 5;

    use ContractsTrait;
    use ProxyTrait;
    use LoggerAwareTrait;

    /**
     * @var AdapterInterface|CacheItemPoolInterface
     */
    private $pool;
    /**
     * @var int
     */
    private $timeout;
    /**
     * @var int
     */
    private $maxNumberOfRetries;
    /**
     * @var int
     */
    private $strategy;
    /**
     * @var int
     */
    private $factor;
    /**
     * @var \Closure
     */
    private $strategyMethod;
    /**
     * @var bool
     */
    private $isContractsTraitUsedByPool;

    /**
     * RetryProxyAdapter constructor.
     *
     * @param AdapterInterface|CacheItemPoolInterface $pool Cache pool
     * @param int $timeout Maximum time to wait, in ms
     * @param int $maxNumberOfRetries Maximum number of retries
     * @param int $strategy Distribution strategy
     * @param float $factor Optional parameter for selected strategy
     */
    public function __construct(CacheItemPoolInterface $pool, int $timeout = 5000, int $maxNumberOfRetries = 4, int $strategy = self::STRATEGY_DEFAULT, float $factor = 3)
    {
        $this->pool = $pool;
        $this->timeout = $timeout;
        $this->maxNumberOfRetries = $maxNumberOfRetries;
        $this->strategy = $strategy;
        $this->factor = $factor;

        $this->setCallbackWrapper(null);
        $this->isContractsTraitUsedByPool = $this->isContractsTraitUsedByPool();

        $this->strategyMethod = [$this, 'noRetryStrategy'];
        if ($this->timeout < 1 || $this->maxNumberOfRetries < 0) {
            CacheItem::log($this->logger, 'Wrong parameters, no retries will be performed');
        } elseif (self::STRATEGY_FLAT_DISTRIBUTION_GEOMETRIC_INTERVALS === $strategy) {
            $this->strategyMethod = [$this, 'flatDistributionGeometricIntervalsStrategy'];
        } elseif (self::STRATEGY_FLAT_DISTRIBUTION_EVEN_INTERVALS === $strategy) {
            $this->strategyMethod = [$this, 'flatDistributionEvenIntervalsStrategy'];
        } elseif (self::STRATEGY_NORMAL_DISTRIBUTION_RANDOM_INTERVALS === $strategy) {
            $this->strategyMethod = [$this, 'normalDistributionRandomIntervalsStrategy'];
        } elseif (self::STRATEGY_DELTA_DISTRIBUTION_EVEN_INTERVALS === $strategy) {
            $this->strategyMethod = [$this, 'deltaDistributionEvenIntervalsStrategy'];
        } elseif (self::STRATEGY_FLAT_DISTRIBUTION_RANDOM_INTERVALS === $strategy) {
            $this->strategyMethod = [$this, 'flatDistributionRandomIntervalsStrategy'];
        } else {
            CacheItem::log($this->logger, 'Unknown strategy "{strategy}", no retries will be performed', ['strategy' => $strategy]);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getItem($key)
    {
        return ($this->strategyMethod)($key);
    }

    /**
     * {@inheritdoc}
     */
    public function getItems(array $keys = [])
    {
        return $this->pool->getItems($keys);
    }

    /**
     * Returns the retrieved item with no retries on misses
     *
     * @param $key
     *
     * @return CacheItemInterface
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function noRetryStrategy($key): CacheItemInterface
    {
        return $this->pool->getItem($key);
    }

    /**
     * Returns a hit as soon as it's retrieved, returns a miss after random number of retries.
     * Zero number of retries is possible to give a chance for earlier computation of a new value.
     *
     * Probability of an early miss and the interval between retries grow in a geometric progression,
     * so while this distribution is on average flat, it produces notable spike at the end of the timeout.
     * To be precise, a proportion of misses that falls in the last interval is (factor - 1) / factor.
     *
     * @param $key
     *
     * @return CacheItemInterface
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function flatDistributionGeometricIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit() || 0 === $this->maxNumberOfRetries) {
            return $item;
        }

        $statTime = microtime(true);
        $base = $this->factor; // aka common ratio
        $randomValueInterval = $timeStep = 1000 * $this->timeout / pow($base, $this->maxNumberOfRetries);
        $randomValue = rand(0, 1000 * $this->timeout - 1);

        // Someone should fail immediately to go to compute the new value for others
        if ($randomValue < $randomValueInterval) {
            // 0 retries
            return $item;
        }

        usleep($timeStep);
        $timeStep = $timeStep * ($base - 1) / $base;
        for ($r = 0; $r < $this->maxNumberOfRetries; $r++) {
            $timeStep *= $base;
            if (0 && ($delta = (microtime(true) - $statTime) * 1000000 + $timeStep - $this->timeout * 1000) > 0) {
                // In order to meet the timeout we need to shrink the last interval
                $timeStep -= $delta;
                if ($timeStep < 0) {
                    // Timeout is too small for performing all retries
                    break;
                }
            }
            usleep($timeStep);
            $item = $this->pool->getItem($key);
            if ($item->isHit()) {
                return $item;
            }
            $randomValueInterval *= $base;
            if ($randomValue < $randomValueInterval) {
                return $item;
            }
        }

        return $item;
    }

    /**
     * Returns a hit as soon as it's retrieved, returns a miss after random number of retries.
     * Zero number of retries is possible to give a chance for earlier computation of a new value.
     *
     * Probability of a miss at every retry and an interval between retries are constant.
     * The distribution of misses is flat on average but not uniform.
     *
     * @param $key
     *
     * @return CacheItemInterface
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function flatDistributionEvenIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit() || 0 === $this->maxNumberOfRetries) {
            return $item;
        }

        $statTime = microtime(true);
        $timeStep = 1000 * $this->timeout / $this->maxNumberOfRetries;
        $numberOfSteps = rand(0, $this->maxNumberOfRetries); // zero included

        for ($r = 0; $r < $numberOfSteps; $r++) {
            if (($delta = (microtime(true) - $statTime) * 1000000 + $timeStep - $this->timeout * 1000) > 0) {
                // In order to meet the timeout we need to shrink the last interval
                $timeStep -= $delta;
                if ($timeStep < 0) {
                    // Timeout is too small for performing all retries
                    break;
                }
            }
            usleep($timeStep);
            $item = $this->pool->getItem($key);
            if ($item->isHit()) {
                return $item;
            }
        }

        return $item;
    }

    /**
     * Returns a hit as soon as it's retrieved, returns a miss after all retries are exceeded.
     * Timeout is shrink randomly in a range from the initial value to a near zero.
     *
     * @param $key
     *
     * @return CacheItemInterface
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function flatDistributionRandomIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit() || 0 === $this->maxNumberOfRetries) {
            return $item;
        }

        $statTime = microtime(true);
        $timeout = rand(1, 1000 * $this->timeout); // flat distribution
        $timeStep = ceil($timeout / $this->maxNumberOfRetries);

        for ($r = 0; $r < $this->maxNumberOfRetries; $r++) {
            if (($delta = (microtime(true) - $statTime) * 1000000 + $timeStep - $this->timeout * 1000) > 0) {
                // In order to meet the timeout we need to shrink the last interval
                $timeStep -= $delta;
                if ($timeStep < 0) {
                    // Timeout is too small for performing all retries
                    break;
                }
            }
            usleep($timeStep);
            $item = $this->pool->getItem($key);
            if ($item->isHit()) {
                return $item;
            }
        }

        return $item;
    }

    /**
     * Returns a hit as soon as it's retrieved, returns a miss after all retries are exceeded.
     *
     * Retries are performed with random time intervals every of which is not longer than overall timeout divided by number of retries.
     * As a result, misses are normally distributed, and mean, mode and median are equal to a half of the overall timeout.
     *
     * @param $key
     *
     * @return CacheItemInterface
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function normalDistributionRandomIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit() || 0 === $this->maxNumberOfRetries) {
            return $item;
        }

        $statTime = microtime(true);

        for ($r = 0; $r < $this->maxNumberOfRetries; $r++) {
            $timeStep = rand(1, 1000 * $this->timeout / $this->maxNumberOfRetries);
            if (($delta = (microtime(true) - $statTime) * 1000000 + $timeStep - $this->timeout * 1000) > 0) {
                // In order to meet the timeout we need to shrink the last interval
                $timeStep -= $delta;
                if ($timeStep < 0) {
                    // Timeout is too small for performing all retries
                    break;
                }
            }
            usleep($timeStep);
            $item = $this->pool->getItem($key);
            if ($item->isHit()) {
                return $item;
            }
        }

        return $item;
    }

    /**
     * Returns a hit as soon as it's retrieved, returns a miss after all retries are exceeded.
     *
     * This distribution is not suitable for stampede protection since it just postpones all misses for a timeout.
     *
     * @param $key
     *
     * @return CacheItemInterface
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function deltaDistributionEvenIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit() || 0 === $this->maxNumberOfRetries) {
            return $item;
        }

        $statTime = microtime(true);
        $timeStep = 1000 * $this->timeout / $this->maxNumberOfRetries;

        for ($r = 0; $r < $this->maxNumberOfRetries; $r++) {
            if (($delta = (microtime(true) - $statTime) * 1000000 + $timeStep - $this->timeout * 1000) > 0) {
                // In order to meet the timeout we need to shrink the last interval
                $timeStep -= $delta;
                if ($timeStep < 0) {
                    // Timeout is too small for performing all retries
                    break;
                }
            }
            usleep($timeStep);
            $item = $this->pool->getItem($key);
            if ($item->isHit()) {
                return $item;
            }
        }

        return $item;
    }

    /**
     * {@inheritdoc}
     */
    public function hasItem($key)
    {
        return $this->pool->hasItem($key);
    }

    /**
     * {@inheritdoc}
     */
    public function clear(string $prefix = '')
    {
        if ($this->pool instanceof AdapterInterface) {
            return $this->pool->clear($prefix);
        }

        return $this->pool->clear();
    }

    /**
     * {@inheritdoc}
     */
    public function deleteItem($key): bool
    {
        return $this->pool->deleteItem($key);
    }

    /**
     * {@inheritdoc}
     */
    public function deleteItems(array $keys): bool
    {
        return $this->pool->deleteItems($keys);
    }

    /**
     * {@inheritdoc}
     */
    public function save(CacheItemInterface $item)
    {
        return $this->pool->save($item);
    }

    /**
     * {@inheritdoc}
     */
    public function saveDeferred(CacheItemInterface $item): bool
    {
        return $this->pool->saveDeferred($item);
    }

    /**
     * {@inheritdoc}
     */
    public function commit()
    {
        return $this->pool->commit();
    }

    /**
     * {@inheritdoc}
     */
    public function get(string $key, callable $callback, float $beta = null, array &$metadata = null)
    {
//        if ($this->isContractsTraitUsedByPool) {
//            $wrapper = $this->pool->setCallbackWrapper(null);
//            $result = $this->pool->get($key, $callback, $beta, $metadata);
//            $this->pool->setCallbackWrapper($wrapper);
//
//            return $result;
//        }

//        if ($this->pool instanceof CacheInterface) {
//            return $this->pool->get($key, $callback, $beta, $metadata);
//        }

        return $this->doGet($this, $key, $callback, $beta, $metadata);
    }

    /**
     * {@inheritdoc}
     */
    public function delete(string $key): bool
    {
//        if ($this->pool instanceof CacheInterface) {
//            return $this->pool->delete($key);
//        }

        return $this->pool->deleteItem($key);
    }

    private function isContractsTraitUsedByPool(): bool
    {
        if (!$this->pool instanceof CacheInterface) {
            return false;
        }

        if (\array_key_exists(ContractsTrait::class, \class_uses($this->pool))) {
            return true;
        }

        foreach (\class_parents($this->pool) as $parentClass) {
            if (\array_key_exists(ContractsTrait::class, \class_uses($parentClass))) {
                return true;
            }
        }

        return false;
    }

}
