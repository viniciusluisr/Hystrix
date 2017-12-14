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
package com.netflix.hystrix.metric;

import com.netflix.hystrix.HystrixInvokableInfo;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.subjects.PublishSubject;
import io.reactivex.subjects.Subject;

import java.util.Collection;

/**
 * Stream of requests, each of which contains a series of command executions
 */
public class HystrixRequestEventsStream {
    private final Subject<HystrixRequestEvents> writeOnlyRequestEventsSubject;
    private final Flowable<HystrixRequestEvents> readOnlyRequestEvents;

    /* package */ HystrixRequestEventsStream() {
        writeOnlyRequestEventsSubject = PublishSubject.create();
        readOnlyRequestEvents = writeOnlyRequestEventsSubject.toFlowable(BackpressureStrategy.BUFFER).onBackpressureBuffer(1024);
    }

    private static final HystrixRequestEventsStream INSTANCE = new HystrixRequestEventsStream();

    public static HystrixRequestEventsStream getInstance() {
        return INSTANCE;
    }

    public void shutdown() {
        writeOnlyRequestEventsSubject.onComplete();
    }

    public void write(Collection<HystrixInvokableInfo<?>> executions) {
        HystrixRequestEvents requestEvents = new HystrixRequestEvents(executions);
        writeOnlyRequestEventsSubject.onNext(requestEvents);
    }

    public Observable<HystrixRequestEvents> observe() {
        return readOnlyRequestEvents.toObservable();
    }
}
