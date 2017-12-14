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
package com.netflix.hystrix.metric.sample;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandMetrics;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.HystrixThreadPoolMetrics;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Consumer;
import io.reactivex.functions.Function;
import org.reactivestreams.Subscription;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class samples current Hystrix utilization of resources and exposes that as a stream
 */
public class HystrixUtilizationStream {
    private final int intervalInMilliseconds;
    private final Flowable<HystrixUtilization> allUtilizationStream;
    private final AtomicBoolean isSourceCurrentlySubscribed = new AtomicBoolean(false);

    private static final DynamicIntProperty dataEmissionIntervalInMs =
            DynamicPropertyFactory.getInstance().getIntProperty("hystrix.stream.utilization.intervalInMilliseconds", 500);


    private static final Function<Long, HystrixUtilization> getAllUtilization =
            new Function<Long, HystrixUtilization>() {
                @Override
                public HystrixUtilization apply(Long timestamp) throws Exception {
                    return HystrixUtilization.from(
                            getAllCommandUtilization.apply(timestamp),
                            getAllThreadPoolUtilization.apply(timestamp)
                    );
                }
            };

    /**
     * @deprecated Not for public use.  Please use {@link #getInstance()}.  This facilitates better stream-sharing
     * @param intervalInMilliseconds milliseconds between data emissions
     */
    @Deprecated //deprecated in 1.5.4.
    public HystrixUtilizationStream(final int intervalInMilliseconds) {
        this.intervalInMilliseconds = intervalInMilliseconds;
        this.allUtilizationStream = Flowable.interval(intervalInMilliseconds, TimeUnit.MILLISECONDS)
                .map(getAllUtilization)
                .doOnSubscribe(new Consumer<Subscription>() {
                    @Override
                    public void accept(Subscription subscription) throws Exception {
                        isSourceCurrentlySubscribed.set(true);
                    }
                })
                .doOnTerminate(new Action() {
                    @Override
                    public void run() {
                        isSourceCurrentlySubscribed.set(false);
                    }
                })
                .share()
                .onBackpressureDrop();
    }

    //The data emission interval is looked up on startup only
    private static final HystrixUtilizationStream INSTANCE =
            new HystrixUtilizationStream(dataEmissionIntervalInMs.get());

    public static HystrixUtilizationStream getInstance() {
        return INSTANCE;
    }

    static HystrixUtilizationStream getNonSingletonInstanceOnlyUsedInUnitTests(int delayInMs) {
        return new HystrixUtilizationStream(delayInMs);
    }

    /**
     * Return a ref-counted stream that will only do work when at least one subscriber is present
     */
    public Observable<HystrixUtilization> observe() {
        return allUtilizationStream.toObservable();
    }

    public Observable<Map<HystrixCommandKey, HystrixCommandUtilization>> observeCommandUtilization() {
        return allUtilizationStream.map(getOnlyCommandUtilization).toObservable();
    }

    public Observable<Map<HystrixThreadPoolKey, HystrixThreadPoolUtilization>> observeThreadPoolUtilization() {
        return allUtilizationStream.map(getOnlyThreadPoolUtilization).toObservable();
    }

    public int getIntervalInMilliseconds() {
        return this.intervalInMilliseconds;
    }

    public boolean isSourceCurrentlySubscribed() {
        return isSourceCurrentlySubscribed.get();
    }

    private static HystrixCommandUtilization sampleCommandUtilization(HystrixCommandMetrics commandMetrics) {
        return HystrixCommandUtilization.sample(commandMetrics);
    }

    private static HystrixThreadPoolUtilization sampleThreadPoolUtilization(HystrixThreadPoolMetrics threadPoolMetrics) {
        return HystrixThreadPoolUtilization.sample(threadPoolMetrics);
    }

    private static final Function<Long, Map<HystrixCommandKey, HystrixCommandUtilization>> getAllCommandUtilization =
            new Function<Long, Map<HystrixCommandKey, HystrixCommandUtilization>>() {
                @Override
                public Map<HystrixCommandKey, HystrixCommandUtilization> apply(Long timestamp) {
                    Map<HystrixCommandKey, HystrixCommandUtilization> commandUtilizationPerKey = new HashMap<HystrixCommandKey, HystrixCommandUtilization>();
                    for (HystrixCommandMetrics commandMetrics: HystrixCommandMetrics.getInstances()) {
                        HystrixCommandKey commandKey = commandMetrics.getCommandKey();
                        commandUtilizationPerKey.put(commandKey, sampleCommandUtilization(commandMetrics));
                    }
                    return commandUtilizationPerKey;
                }
            };

    private static final Function<Long, Map<HystrixThreadPoolKey, HystrixThreadPoolUtilization>> getAllThreadPoolUtilization =
            new Function<Long, Map<HystrixThreadPoolKey, HystrixThreadPoolUtilization>>() {
                @Override
                public Map<HystrixThreadPoolKey, HystrixThreadPoolUtilization> apply(Long timestamp) {
                    Map<HystrixThreadPoolKey, HystrixThreadPoolUtilization> threadPoolUtilizationPerKey = new HashMap<HystrixThreadPoolKey, HystrixThreadPoolUtilization>();
                    for (HystrixThreadPoolMetrics threadPoolMetrics: HystrixThreadPoolMetrics.getInstances()) {
                        HystrixThreadPoolKey threadPoolKey = threadPoolMetrics.getThreadPoolKey();
                        threadPoolUtilizationPerKey.put(threadPoolKey, sampleThreadPoolUtilization(threadPoolMetrics));
                    }
                    return threadPoolUtilizationPerKey;
                }
            };

    private static final Function<HystrixUtilization, Map<HystrixCommandKey, HystrixCommandUtilization>> getOnlyCommandUtilization =
            new Function<HystrixUtilization, Map<HystrixCommandKey, HystrixCommandUtilization>>() {
                @Override
                public Map<HystrixCommandKey, HystrixCommandUtilization> apply(HystrixUtilization hystrixUtilization) {
                    return hystrixUtilization.getCommandUtilizationMap();
                }
            };

    private static final Function<HystrixUtilization, Map<HystrixThreadPoolKey, HystrixThreadPoolUtilization>> getOnlyThreadPoolUtilization =
            new Function<HystrixUtilization, Map<HystrixThreadPoolKey, HystrixThreadPoolUtilization>>() {
                @Override
                public Map<HystrixThreadPoolKey, HystrixThreadPoolUtilization> apply(HystrixUtilization hystrixUtilization) {
                    return hystrixUtilization.getThreadPoolUtilizationMap();
                }
            };
}
