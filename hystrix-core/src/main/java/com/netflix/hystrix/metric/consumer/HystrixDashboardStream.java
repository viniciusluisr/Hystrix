/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.hystrix.metric.consumer;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.hystrix.HystrixCollapserMetrics;
import com.netflix.hystrix.HystrixCommandMetrics;
import com.netflix.hystrix.HystrixThreadPoolMetrics;
import io.reactivex.Flowable;
import io.reactivex.Observable;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HystrixDashboardStream {
    final int delayInMs;
    final Flowable<DashboardData> singleSource;
    final AtomicBoolean isSourceCurrentlySubscribed = new AtomicBoolean(false);

    private static final DynamicIntProperty dataEmissionIntervalInMs =
            DynamicPropertyFactory.getInstance().getIntProperty("hystrix.stream.dashboard.intervalInMilliseconds", 500);

    private HystrixDashboardStream(int delayInMs) {
        this.delayInMs = delayInMs;
        this.singleSource = Flowable.interval(delayInMs, TimeUnit.MILLISECONDS)
                .map(timestamp -> new DashboardData(
                        HystrixCommandMetrics.getInstances(),
                        HystrixThreadPoolMetrics.getInstances(),
                        HystrixCollapserMetrics.getInstances()
                ))
                .doOnSubscribe(disposable -> isSourceCurrentlySubscribed.set(true))
                .doOnTerminate(() -> isSourceCurrentlySubscribed.set(false))
                .share()
                .onBackpressureDrop();
    }

    //The data emission interval is looked up on startup only
    private static final HystrixDashboardStream INSTANCE =
            new HystrixDashboardStream(dataEmissionIntervalInMs.get());

    public static HystrixDashboardStream getInstance() {
        return INSTANCE;
    }

    static HystrixDashboardStream getNonSingletonInstanceOnlyUsedInUnitTests(int delayInMs) {
        return new HystrixDashboardStream(delayInMs);
    }

    /**
     * Return a ref-counted stream that will only do work when at least one subscriber is present
     */
    public Observable<DashboardData> observe() {
        return singleSource.toObservable();
    }

    public boolean isSourceCurrentlySubscribed() {
        return isSourceCurrentlySubscribed.get();
    }

    public static class DashboardData {
        final Collection<HystrixCommandMetrics> commandMetrics;
        final Collection<HystrixThreadPoolMetrics> threadPoolMetrics;
        final Collection<HystrixCollapserMetrics> collapserMetrics;

        public DashboardData(Collection<HystrixCommandMetrics> commandMetrics, Collection<HystrixThreadPoolMetrics> threadPoolMetrics, Collection<HystrixCollapserMetrics> collapserMetrics) {
            this.commandMetrics = commandMetrics;
            this.threadPoolMetrics = threadPoolMetrics;
            this.collapserMetrics = collapserMetrics;
        }

        public Collection<HystrixCommandMetrics> getCommandMetrics() {
            return commandMetrics;
        }

        public Collection<HystrixThreadPoolMetrics> getThreadPoolMetrics() {
            return threadPoolMetrics;
        }

        public Collection<HystrixCollapserMetrics> getCollapserMetrics() {
            return collapserMetrics;
        }
    }
}


