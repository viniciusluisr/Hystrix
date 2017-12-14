/**
 * Copyright 2015 Netflix, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.hystrix.metric.consumer;

import com.netflix.hystrix.metric.HystrixEvent;
import com.netflix.hystrix.metric.HystrixEventStream;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.functions.BiFunction;
import io.reactivex.functions.Function;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Refinement of {@link BucketedCounterStream} which reduces numBuckets at a time.
 *
 * @param <Event> type of raw data that needs to get summarized into a bucket
 * @param <Bucket> type of data contained in each bucket
 * @param <Output> type of data emitted to stream subscribers (often is the same as A but does not have to be)
 */
public abstract class BucketedRollingCounterStream<Event extends HystrixEvent, Bucket, Output> extends BucketedCounterStream<Event, Bucket, Output> {
    private Flowable<Output> sourceStream;
    private final AtomicBoolean isSourceCurrentlySubscribed = new AtomicBoolean(false);

    protected BucketedRollingCounterStream(HystrixEventStream<Event> stream, final int numBuckets, int bucketSizeInMs,
                                           final BiFunction<Bucket, Event, Bucket> appendRawEventToBucket,
                                           final BiFunction<Output, Bucket, Output> reduceBucket) {
        super(stream, numBuckets, bucketSizeInMs, appendRawEventToBucket);
        Function<Flowable<Bucket>, Flowable<Output>> reduceWindowToSummary = window -> window.scan(getEmptyOutputValue(), reduceBucket).skip(numBuckets);
        this.sourceStream = bucketedStream      //stream broken up into buckets
                .window(numBuckets, 1)          //emit overlapping windows of buckets
                .flatMap(reduceWindowToSummary) //convert a window of bucket-summaries into a single summary
                .doOnSubscribe(subscription -> isSourceCurrentlySubscribed.set(true))
                .doOnTerminate(() -> isSourceCurrentlySubscribed.set(false))
                .share()                        //multiple subscribers should get same data
                .onBackpressureDrop();          //if there are slow consumers, data should not buffer
    }

    @Override
    public Observable<Output> observe() {
        return sourceStream.toObservable();
    }

    /* package-private */ boolean isSourceCurrentlySubscribed() {
        return isSourceCurrentlySubscribed.get();
    }
}
