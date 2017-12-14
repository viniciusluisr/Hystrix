/**
 * Copyright 2015 Netflix, Inc.
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

import com.netflix.hystrix.HystrixCollapserKey;
import io.reactivex.Observable;
import io.reactivex.subjects.BehaviorSubject;
import io.reactivex.subjects.Subject;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Per-Collapser stream of {@link HystrixCollapserEvent}s.  This gets written to by {@link HystrixThreadEventStream}s.
 * Events are emitted synchronously in the same thread that performs the batch-command execution.
 */
public class HystrixCollapserEventStream implements HystrixEventStream<HystrixCollapserEvent> {
    private final HystrixCollapserKey collapserKey;

    private final Subject<HystrixCollapserEvent> writeOnlyStream;
    private final Observable<HystrixCollapserEvent> readOnlyStream;

    private static final ConcurrentMap<String, HystrixCollapserEventStream> streams = new ConcurrentHashMap<String, HystrixCollapserEventStream>();

    public static HystrixCollapserEventStream getInstance(HystrixCollapserKey collapserKey) {
        HystrixCollapserEventStream initialStream = streams.get(collapserKey.name());
        if (initialStream != null) {
            return initialStream;
        } else {
            synchronized (HystrixCollapserEventStream.class) {
                HystrixCollapserEventStream existingStream = streams.get(collapserKey.name());
                if (existingStream == null) {
                    HystrixCollapserEventStream newStream = new HystrixCollapserEventStream(collapserKey);
                    streams.putIfAbsent(collapserKey.name(), newStream);
                    return newStream;
                } else {
                    return existingStream;
                }
            }
        }
    }

    HystrixCollapserEventStream(final HystrixCollapserKey collapserKey) {
        this.collapserKey = collapserKey;
        this.writeOnlyStream = BehaviorSubject.<HystrixCollapserEvent>create().toSerialized();
        this.readOnlyStream = writeOnlyStream.share();
    }

    public static void reset() {
        streams.clear();
    }

    public void write(HystrixCollapserEvent event) {
        writeOnlyStream.onNext(event);
    }

    public Observable<HystrixCollapserEvent> observe() {
        return readOnlyStream;
    }

    @Override
    public String toString() {
        return "HystrixCollapserEventStream(" + collapserKey.name() + ")";
    }
}
