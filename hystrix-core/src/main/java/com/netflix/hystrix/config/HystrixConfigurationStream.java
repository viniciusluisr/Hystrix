/**
 * Copyright 2016 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.hystrix.config;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCollapserKey;
import com.netflix.hystrix.HystrixCollapserMetrics;
import com.netflix.hystrix.HystrixCollapserProperties;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandMetrics;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.HystrixThreadPoolMetrics;
import com.netflix.hystrix.HystrixThreadPoolProperties;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class samples current Hystrix configuration and exposes that as a stream
 */
public class HystrixConfigurationStream {

    private final int intervalInMilliseconds;
    private final Flowable<HystrixConfiguration> allConfigurationStream;
    private final AtomicBoolean isSourceCurrentlySubscribed = new AtomicBoolean(false);

    private static final DynamicIntProperty dataEmissionIntervalInMs =
            DynamicPropertyFactory.getInstance().getIntProperty("hystrix.stream.config.intervalInMilliseconds", 5000);


    private static final Function<Long, HystrixConfiguration> getAllConfig =
            new Function<Long, HystrixConfiguration>() {
                @Override
                public HystrixConfiguration apply(Long timestamp) throws Exception {
                    return HystrixConfiguration.from(
                            getAllCommandConfig.apply(timestamp),
                            getAllThreadPoolConfig.apply(timestamp),
                            getAllCollapserConfig.apply(timestamp)
                    );
                }
            };

    /**
     * @deprecated Not for public use.  Please use {@link #getInstance()}.  This facilitates better stream-sharing
     * @param intervalInMilliseconds milliseconds between data emissions
     */
    @Deprecated //deprecated in 1.5.4.
    public HystrixConfigurationStream(final int intervalInMilliseconds) {
        this.intervalInMilliseconds = intervalInMilliseconds;
        this.allConfigurationStream = Flowable.interval(intervalInMilliseconds, TimeUnit.MILLISECONDS)
                .map(getAllConfig)
                .doOnSubscribe(a -> isSourceCurrentlySubscribed.set(true))
                .doOnTerminate(() -> isSourceCurrentlySubscribed.set(false))
                .share()
                .onBackpressureDrop();
    }

    //The data emission interval is looked up on startup only
    private static final HystrixConfigurationStream INSTANCE =
            new HystrixConfigurationStream(dataEmissionIntervalInMs.get());

    public static HystrixConfigurationStream getInstance() {
        return INSTANCE;
    }

    static HystrixConfigurationStream getNonSingletonInstanceOnlyUsedInUnitTests(int delayInMs) {
        return new HystrixConfigurationStream(delayInMs);
    }

    /**
     * Return a ref-counted stream that will only do work when at least one subscriber is present
     */
    public Flowable<HystrixConfiguration> observe() {
        return allConfigurationStream;
    }

    public Flowable<Map<HystrixCommandKey, HystrixCommandConfiguration>> observeCommandConfiguration() {
        return allConfigurationStream.map(getOnlyCommandConfig);
    }

    public Flowable<Map<HystrixThreadPoolKey, HystrixThreadPoolConfiguration>> observeThreadPoolConfiguration() {
        return allConfigurationStream.map(getOnlyThreadPoolConfig);
    }

    public Flowable<Map<HystrixCollapserKey, HystrixCollapserConfiguration>> observeCollapserConfiguration() {
        return allConfigurationStream.map(getOnlyCollapserConfig);
    }

    public int getIntervalInMilliseconds() {
        return this.intervalInMilliseconds;
    }

    public boolean isSourceCurrentlySubscribed() {
        return isSourceCurrentlySubscribed.get();
    }

    private static HystrixCommandConfiguration sampleCommandConfiguration(HystrixCommandKey commandKey, HystrixThreadPoolKey threadPoolKey,
                                                                          HystrixCommandGroupKey groupKey, HystrixCommandProperties commandProperties) {
        return HystrixCommandConfiguration.sample(commandKey, threadPoolKey, groupKey, commandProperties);
    }

    private static HystrixThreadPoolConfiguration sampleThreadPoolConfiguration(HystrixThreadPoolKey threadPoolKey, HystrixThreadPoolProperties threadPoolProperties) {
        return HystrixThreadPoolConfiguration.sample(threadPoolKey, threadPoolProperties);
    }

    private static HystrixCollapserConfiguration sampleCollapserConfiguration(HystrixCollapserKey collapserKey, HystrixCollapserProperties collapserProperties) {
        return HystrixCollapserConfiguration.sample(collapserKey, collapserProperties);
    }

    private static final Function<Long, Map<HystrixCommandKey, HystrixCommandConfiguration>> getAllCommandConfig =
            timestamp -> {
                Map<HystrixCommandKey, HystrixCommandConfiguration> commandConfigPerKey = new HashMap<>();
                for (HystrixCommandMetrics commandMetrics: HystrixCommandMetrics.getInstances()) {
                    HystrixCommandKey commandKey = commandMetrics.getCommandKey();
                    HystrixThreadPoolKey threadPoolKey = commandMetrics.getThreadPoolKey();
                    HystrixCommandGroupKey groupKey = commandMetrics.getCommandGroup();
                    commandConfigPerKey.put(commandKey, sampleCommandConfiguration(commandKey, threadPoolKey, groupKey, commandMetrics.getProperties()));
                }
                return commandConfigPerKey;
            };

    private static final Function<Long, Map<HystrixThreadPoolKey, HystrixThreadPoolConfiguration>> getAllThreadPoolConfig =
            timestamp -> {
                Map<HystrixThreadPoolKey, HystrixThreadPoolConfiguration> threadPoolConfigPerKey = new HashMap<>();
                for (HystrixThreadPoolMetrics threadPoolMetrics: HystrixThreadPoolMetrics.getInstances()) {
                    HystrixThreadPoolKey threadPoolKey = threadPoolMetrics.getThreadPoolKey();
                    threadPoolConfigPerKey.put(threadPoolKey, sampleThreadPoolConfiguration(threadPoolKey, threadPoolMetrics.getProperties()));
                }
                return threadPoolConfigPerKey;
            };

    private static final Function<Long, Map<HystrixCollapserKey, HystrixCollapserConfiguration>> getAllCollapserConfig =
            timestamp -> {
                Map<HystrixCollapserKey, HystrixCollapserConfiguration> collapserConfigPerKey = new HashMap<>();
                for (HystrixCollapserMetrics collapserMetrics: HystrixCollapserMetrics.getInstances()) {
                    HystrixCollapserKey collapserKey = collapserMetrics.getCollapserKey();
                    collapserConfigPerKey.put(collapserKey, sampleCollapserConfiguration(collapserKey, collapserMetrics.getProperties()));
                }
                return collapserConfigPerKey;
            };



    private static final Function<HystrixConfiguration, Map<HystrixCommandKey, HystrixCommandConfiguration>> getOnlyCommandConfig =
            HystrixConfiguration::getCommandConfig;

    private static final Function<HystrixConfiguration, Map<HystrixThreadPoolKey, HystrixThreadPoolConfiguration>> getOnlyThreadPoolConfig =
            HystrixConfiguration::getThreadPoolConfig;

    private static final Function<HystrixConfiguration, Map<HystrixCollapserKey, HystrixCollapserConfiguration>> getOnlyCollapserConfig =
            HystrixConfiguration::getCollapserConfig;
}
