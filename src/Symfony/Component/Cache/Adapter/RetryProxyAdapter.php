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
 * Proxies calls to methods of AdapterInterface::class or any other PSR-6 compliant adapter
 * and performs retries when getting of individual items fails. All other calls, including getItems(),
 * are just forwarded to underlying adapter without retries.
 *
 * CacheInterface::class is completely implemented in the proxy adapter to leverage retries
 * so no calls to its methods are forwarded.
 *
 * Features:
 * Allowed number of retries are distributed over the timeout interval according to chosen distribution strategy.
 * Actual number of retries depends on strategy and may be less than allowed number, down to zero.
 * Compliance with timeout has higher priority over number of retries.
 *
 * @author Sergey Belyshkin <sbelyshkin@gmail.com>
 */
class RetryProxyAdapter implements AdapterInterface, CacheInterface, LoggerAwareInterface, PruneableInterface, ResettableInterface
{
    use ContractsTrait;
    use LoggerAwareTrait;
    use ProxyTrait;

    public const STRATEGY_DEFAULT = self::STRATEGY_FLAT_DISTRIBUTION_GEOMETRIC_INTERVALS;
    public const STRATEGY_NORMAL_DISTRIBUTION_RANDOM_INTERVALS = 1;
    public const STRATEGY_FLAT_DISTRIBUTION_EVEN_INTERVALS = 2;
    public const STRATEGY_FLAT_DISTRIBUTION_GEOMETRIC_INTERVALS = 3;
    public const STRATEGY_DELTA_DISTRIBUTION_EVEN_INTERVALS = 4;
    public const STRATEGY_FLAT_DISTRIBUTION_RANDOM_INTERVALS = 5;
    public const STRATEGY_BINOMIAL_DISTRIBUTION_EVEN_INTERVALS = 6;

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
     * @param AdapterInterface|CacheItemPoolInterface $pool               Cache pool
     * @param int                                     $timeout            Maximum time to wait, in ms
     * @param int                                     $maxNumberOfRetries Maximum number of retries
     * @param int                                     $strategy           Distribution strategy
     * @param float                                   $factor             Optional parameter for selected strategy
     */
    public function __construct(CacheItemPoolInterface $pool, int $timeout = 5000, int $maxNumberOfRetries = 4, int $strategy = self::STRATEGY_DEFAULT, float $factor = 3)
    {
        $this->pool = $pool;
        $this->timeout = $timeout;
        $this->maxNumberOfRetries = $maxNumberOfRetries;
        $this->strategy = $strategy;
        $this->factor = $factor;

        $this->setCallbackWrapper(null); // Adapter needs to be free of locks by design

        $this->strategyMethod = [$this, 'noRetryStrategy'];
        if ($this->timeout < 1 || $this->maxNumberOfRetries < 0) {
            CacheItem::log(
                $this->logger,
                'Wrong configuration of retries for "{adapter}": "{timeout}"/"{maxNumberOfRetries}"/"{strategy}", no retries will be performed',
                ['adapter' => get_debug_type($this->pool), 'timeout' => $this->timeout, 'maxNumberOfRetries' => $this->maxNumberOfRetries, 'strategy' => $this->strategy]
            );
        } elseif (0 === $this->maxNumberOfRetries) {
            CacheItem::log($this->logger, 'Adapter was configured with zero retries for {adapter}', ['adapter' => get_debug_type($this->pool)]);
        } elseif (self::STRATEGY_FLAT_DISTRIBUTION_GEOMETRIC_INTERVALS === $strategy) {
            if ($this->factor > 0) {
                $this->strategyMethod = [$this, 'flatDistributionGeometricIntervalsStrategy'];
            } else {
                $this->strategyMethod = [$this, 'noRetryStrategy'];
                CacheItem::log(
                    $this->logger,
                    'Wrong parameter value "{factor}" for chosen strategy "{strategy}", no retries will be performed',
                    ['factor' => $this->factor, 'strategy' => $this->strategy]
                );
            }
        } elseif (self::STRATEGY_BINOMIAL_DISTRIBUTION_EVEN_INTERVALS === $strategy) {
            if ($this->factor < 0 || $this->factor > 1 && $this->factor >= $this->maxNumberOfRetries) {
                // factor <= 0 turns retries off, factor >= 1 leads to delta-distribution
                CacheItem::log(
                    $this->logger,
                    'Parameter values "{factor}"/"{maxNumberOfRetries}" are out of the proper range for chosen strategy "{strategy}", please check',
                    ['factor' => $this->factor, 'maxNumberOfRetries' => $this->maxNumberOfRetries, 'strategy' => $this->strategy]
                );
            }
            $this->strategyMethod = [$this, 'binomialDistributionEvenIntervalsStrategy'];
        } elseif (self::STRATEGY_FLAT_DISTRIBUTION_EVEN_INTERVALS === $strategy) {
            $this->strategyMethod = [$this, 'flatDistributionEvenIntervalsStrategy'];
        } elseif (self::STRATEGY_NORMAL_DISTRIBUTION_RANDOM_INTERVALS === $strategy) {
            $this->strategyMethod = [$this, 'normalDistributionRandomIntervalsStrategy'];
        } elseif (self::STRATEGY_DELTA_DISTRIBUTION_EVEN_INTERVALS === $strategy) {
            $this->strategyMethod = [$this, 'deltaDistributionEvenIntervalsStrategy'];
        } elseif (self::STRATEGY_FLAT_DISTRIBUTION_RANDOM_INTERVALS === $strategy) {
            $this->strategyMethod = [$this, 'flatDistributionRandomIntervalsStrategy'];
        } else {
            CacheItem::log($this->logger, 'Unknown strategy "{strategy}", no retries will be performed', ['strategy' => $this->strategy]);
        }
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
    public function commit()
    {
        return $this->pool->commit();
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
    public function hasItem($key)
    {
        return $this->pool->hasItem($key);
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
    public function get(string $key, callable $callback, float $beta = null, array &$metadata = null)
    {
        return $this->doGet($this, $key, $callback, $beta, $metadata);
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
     * Returns the retrieved item with no retries on misses.
     *
     * @param $key
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
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function flatDistributionGeometricIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit()) {
            return $item;
        }

        $startTime = microtime(true);
        $base = $this->factor; // aka common ratio
        $randomValueInterval = $timeStep = 1000 * $this->timeout / $base ** $this->maxNumberOfRetries;
        $randomValue = rand(0, 1000 * $this->timeout - 1);

        // Someone should fail immediately to go to compute the new value for others awaiting it
        if ($randomValue < $randomValueInterval) {
            // 0 retries
            return $item;
        }

        usleep($timeStep);
        $timeStep = $timeStep * ($base - 1) / $base;
        $retryCounter = 9;
        for ($r = 0; $r < $this->maxNumberOfRetries; ++$r) {
            $timeStep *= $base;
            $timeStep = $this->getAdjustedTimeInterval($timeStep, $startTime, ++$retryCounter);
            if ($timeStep < 0) {
                break;
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
     * It's a discrete uniform distribution. Since the case with zero retries included,
     * proportion of misses on every retry is 1 / (1 + maxNumberOfRetries).
     *
     * @param $key
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function flatDistributionEvenIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit()) {
            return $item;
        }

        $startTime = microtime(true);
        $timeStep = 1000 * $this->timeout / $this->maxNumberOfRetries;
        $numberOfSteps = rand(0, $this->maxNumberOfRetries); // zero included

        $retryCounter = 0;
        for ($r = 0; $r < $numberOfSteps; ++$r) {
            $timeStep = $this->getAdjustedTimeInterval($timeStep, $startTime, ++$retryCounter);
            if ($timeStep < 0) {
                break;
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
     * Timeout is shrink randomly from the initial value down to a near zero.
     *
     * @param $key
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function flatDistributionRandomIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit()) {
            return $item;
        }

        $startTime = microtime(true);
        $timeout = rand(1, 1000 * $this->timeout); // uniform distribution of misses within initial timeout interval
        $timeStep = ceil($timeout / $this->maxNumberOfRetries);

        $retryCounter = 0;
        for ($r = 0; $r < $this->maxNumberOfRetries; ++$r) {
            $timeStep = $this->getAdjustedTimeInterval($timeStep, $startTime, ++$retryCounter);
            if ($timeStep < 0) {
                break;
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
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function normalDistributionRandomIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit()) {
            return $item;
        }

        $startTime = microtime(true);

        $retryCounter = 0;
        for ($r = 0; $r < $this->maxNumberOfRetries; ++$r) {
            $timeStep = rand(1, 1000 * $this->timeout / $this->maxNumberOfRetries);
            $timeStep = $this->getAdjustedTimeInterval($timeStep, $startTime, ++$retryCounter);
            if ($timeStep < 0) {
                break;
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
     * This distribution is not suitable for cache stampede protection when get-computeIfAbsent-put pattern is used
     * since it just postpones all misses and no single caller computes new value before expiration of the timeout.
     *
     * @param $key
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function deltaDistributionEvenIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit()) {
            return $item;
        }

        $startTime = microtime(true);
        $timeStep = 1000 * $this->timeout / $this->maxNumberOfRetries;

        $retryCounter = 0;
        for ($r = 0; $r < $this->maxNumberOfRetries; ++$r) {
            $timeStep = $this->getAdjustedTimeInterval($timeStep, $startTime, ++$retryCounter);
            if ($timeStep < 0) {
                break;
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
     * Returns a hit as soon as it's retrieved, returns a miss after random number of retries.
     * Zero number of retries is possible to give a chance for earlier computation of a new value.
     *
     * Time intervals between retries are equal as well as the probability of each retry.
     *
     * @param $key
     *
     * @throws \Psr\Cache\InvalidArgumentException
     */
    private function binomialDistributionEvenIntervalsStrategy($key): CacheItemInterface
    {
        $item = $this->pool->getItem($key);
        if ($item->isHit()) {
            return $item;
        }

        $startTime = microtime(true);
        $timeStep = 1000 * $this->timeout / $this->maxNumberOfRetries;

        if ($this->factor < 0) {
            $probabilityOfEachRetry = 0;
        } elseif ($this->factor < 1) {
            $probabilityOfEachRetry = $this->factor;
        } elseif ($this->factor < $this->maxNumberOfRetries) {
            // The factor here is the expected number of successes in a sequence of N tries,
            $probabilityOfEachRetry = $this->factor / $this->maxNumberOfRetries;
        } else {
            $probabilityOfEachRetry = 1;
        }

        $numberOfRetries = $this->maxNumberOfRetries;
        $retryCounter = 0;
        for ($r = 0; $r < $numberOfRetries; ++$r) {
            // Probability is in the range [0; 1], 0 means never, 1 means always
            if (($probabilityOfEachRetry * (1 << 20)) < (rand(1, 1 << 20))) {
                continue;
            }
            $timeStep = $this->getAdjustedTimeInterval($timeStep, $startTime, ++$retryCounter);
            if ($timeStep < 0) {
                break;
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
     * Adjusts (shrinks) time interval if it doesn't comply with overall time limit.
     * If returned interval is negative then the timeout has already been exceeded and caller should stop retrying.
     *
     * @param float $nextTimeInterval Time interval in microseconds
     * @param float $startTime        Timestamp
     *
     * @return float Adjusted time interval in microseconds
     */
    private function getAdjustedTimeInterval(float $nextTimeInterval, float $startTime, int $retryNumber): float
    {
        $delta = (microtime(true) - $startTime) * 1000000 + $nextTimeInterval - $this->timeout * 1000;
        if ($delta > 0) {
            // In order to meet the timeout we need to shrink next (most likely the last) interval
            $nextTimeInterval -= $delta;
            if ($nextTimeInterval < 0) {
                CacheItem::log(
                    $this->logger,
                    'Timeout "{timeout}" is too small to perform all "{maxNumberOfRetries}" retries for "{adapter}" with strategy "{strategy}", retry #{retryNumber} won\'t be performed',
                    ['adapter' => get_debug_type($this->pool), 'timeout' => $this->timeout, 'maxNumberOfRetries' => $this->maxNumberOfRetries, 'strategy' => $this->strategy, 'retryNumber' => $retryNumber]
                );
            }
        }

        return $nextTimeInterval;
    }
}
