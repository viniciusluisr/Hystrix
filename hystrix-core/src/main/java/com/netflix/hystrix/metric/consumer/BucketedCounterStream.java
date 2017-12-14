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
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Function;
import io.reactivex.functions.BiFunction;
import io.reactivex.subjects.BehaviorSubject;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Abstract class that imposes a bucketing structure and provides streams of buckets
 *
 * @param <Event> type of raw data that needs to get summarized into a bucket
 * @param <Bucket> type of data contained in each bucket
 * @param <Output> type of data emitted to stream subscribers (often is the same as A but does not have to be)
 */
public abstract class BucketedCounterStream<Event extends HystrixEvent, Bucket, Output> {
    protected final int numBuckets;
    protected final Flowable<Bucket> bucketedStream;
    protected final AtomicReference<Subscription> subscription = new AtomicReference<>(null);

    private final Function<Flowable<Event>, Flowable<Bucket>> reduceBucketToSummary;

    private final BehaviorSubject<Output> counterSubject = BehaviorSubject.create(getEmptyOutputValue());

    protected BucketedCounterStream(final HystrixEventStream<Event> inputEventStream, final int numBuckets, final int bucketSizeInMs,
                                    final BiFunction<Bucket, Event, Bucket> appendRawEventToBucket) {
        this.numBuckets = numBuckets;
        this.reduceBucketToSummary = (Function<Observable<Event>, Observable<Bucket>>) eventBucket -> eventBucket.reduce(getEmptyBucketSummary(), appendRawEventToBucket);

        final List<Bucket> emptyEventCountsToStart = new ArrayList<>();
        for (int i = 0; i < numBuckets; i++) {
            emptyEventCountsToStart.add(getEmptyBucketSummary());
        }

        this.bucketedStream = Flowable.defer(() -> {
            return inputEventStream
                    .observe()
                    .toFlowable(BackpressureStrategy.BUFFER)
                    .window(bucketSizeInMs, TimeUnit.MILLISECONDS) //bucket it by the counter window so we can emit to the next operator in time chunks, not on every OnNext
                    .flatMap(reduceBucketToSummary)                //for a given bucket, turn it into a long array containing counts of event types
                    .startWith(emptyEventCountsToStart);           //start it with empty arrays to make consumer logic as generic as possible (windows are always full)
        });
    }

    abstract Bucket getEmptyBucketSummary();

    abstract Output getEmptyOutputValue();

    /**
     * Return the stream of buckets
     * @return stream of buckets
     */
    public abstract Observable<Output> observe();

    public void startCachingStreamValuesIfUnstarted() {
        if (subscription.get() == null) {
            //the stream is not yet started
            Subscription candidateSubscription = observe().subscribe(counterSubject);
            if (subscription.compareAndSet(null, candidateSubscription)) {
                //won the race to set the subscription
            } else {
                //lost the race to set the subscription, so we need to cancel this one
                candidateSubscription.unsubscribe();
            }
        }
    }

    /**
     * Synchronous call to retrieve the last calculated bucket without waiting for any emissions
     * @return last calculated bucket
     */
    public Output getLatest() {
        startCachingStreamValuesIfUnstarted();
        if (counterSubject.hasValue()) {
            return counterSubject.getValue();
        } else {
            return getEmptyOutputValue();
        }
    }

    public void unsubscribe() {
        Subscription s = subscription.get();
        if (s != null) {
            s.unsubscribe();
            subscription.compareAndSet(s, null);
        }
    }
}
