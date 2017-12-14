/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.hystrix.strategy.concurrency;

import java.util.concurrent.*;

import io.reactivex.*;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.reactivex.disposables.Disposables;
import io.reactivex.disposables.SerialDisposable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Function;
import io.reactivex.disposables.Disposables.*;

import com.netflix.hystrix.HystrixThreadPool;
import com.netflix.hystrix.strategy.HystrixPlugins;

/**
 * Wrap a {@link Scheduler} so that scheduled actions are wrapped with {@link HystrixContexSchedulerAction} so that
 * the {@link HystrixRequestContext} is properly copied across threads (if they are used by the {@link Scheduler}).
 */
public class HystrixContextScheduler extends Scheduler {

    private final HystrixConcurrencyStrategy concurrencyStrategy;
    private final Scheduler actualScheduler;
    private final HystrixThreadPool threadPool;

    public HystrixContextScheduler(Scheduler scheduler) {
        this.actualScheduler = scheduler;
        this.concurrencyStrategy = HystrixPlugins.getInstance().getConcurrencyStrategy();
        this.threadPool = null;
    }

    public HystrixContextScheduler(HystrixConcurrencyStrategy concurrencyStrategy, Scheduler scheduler) {
        this.actualScheduler = scheduler;
        this.concurrencyStrategy = concurrencyStrategy;
        this.threadPool = null;
    }

    public HystrixContextScheduler(HystrixConcurrencyStrategy concurrencyStrategy, HystrixThreadPool threadPool) {
        this(concurrencyStrategy, threadPool, new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return true;
            }
        });
    }

    public HystrixContextScheduler(HystrixConcurrencyStrategy concurrencyStrategy, HystrixThreadPool threadPool, Callable<Boolean> shouldInterruptThread) {
        this.concurrencyStrategy = concurrencyStrategy;
        this.threadPool = threadPool;
        this.actualScheduler = new ThreadPoolScheduler(threadPool, shouldInterruptThread);
    }

    @Override
    public Worker createWorker() {
        return new HystrixContextSchedulerWorker(actualScheduler.createWorker());
    }

    private class HystrixContextSchedulerWorker extends Worker {

        private final Worker worker;

        private HystrixContextSchedulerWorker(Worker actualWorker) {
            this.worker = actualWorker;
        }

        @Override
        public void dispose() {
            worker.dispose();
        }

        @Override
        public boolean isDisposed() {
            return worker.isDisposed();
        }

        @Override
        public Disposable schedule(Callable action, long delayTime, TimeUnit unit) {
            if (threadPool != null) {
                if (!threadPool.isQueueSpaceAvailable()) {
                    throw new RejectedExecutionException("Rejected command because thread-pool queueSize is at rejection threshold.");
                }
            }
            return worker.schedule(new HystrixContexSchedulerAction(concurrencyStrategy, action), delayTime, unit);
        }

        @Override
        public Disposable schedule(Callable action) {
            if (threadPool != null) {
                if (!threadPool.isQueueSpaceAvailable()) {
                    throw new RejectedExecutionException("Rejected command because thread-pool queueSize is at rejection threshold.");
                }
            }
            return worker.schedule(new HystrixContexSchedulerAction(concurrencyStrategy, action));
        }

    }

    private static class ThreadPoolScheduler extends Scheduler {

        private final HystrixThreadPool threadPool;
        private final Callable<Boolean> shouldInterruptThread;

        public ThreadPoolScheduler(HystrixThreadPool threadPool, Callable<Boolean> shouldInterruptThread) {
            this.threadPool = threadPool;
            this.shouldInterruptThread = shouldInterruptThread;
        }

        @Override
        public Worker createWorker() {
            return new ThreadPoolWorker(threadPool, shouldInterruptThread);
        }

    }

    /**
     * Purely for scheduling work on a thread-pool.
     * <p>
     * This is not natively supported by RxJava as of 0.18.0 because thread-pools
     * are contrary to sequential execution.
     * <p>
     * For the Hystrix case, each Command invocation has a single action so the concurrency
     * issue is not a problem.
     */
    private static class ThreadPoolWorker extends Worker {

        private final HystrixThreadPool threadPool;
        private final CompositeDisposable subscription = new CompositeDisposable();
        private final Callable<Boolean> shouldInterruptThread;

        public ThreadPoolWorker(HystrixThreadPool threadPool, Callable<Boolean> shouldInterruptThread) {
            this.threadPool = threadPool;
            this.shouldInterruptThread = shouldInterruptThread;
        }

        @Override
        public void dispose() {
            subscription.dispose();
        }

        @Override
        public boolean isDisposed() {
            return subscription.isDisposed();
        }

        public Disposable schedule(final Action action) {
            if (subscription.isDisposed()) {
                // don't schedule, we are unsubscribed
                return Disposables.disposed();
            }

            // This is internal RxJava API but it is too useful.
            CompositeDisposable sa = (CompositeDisposable) Disposables.fromAction(action);

            subscription.add(sa);

            sa.add(subscription);

            ThreadPoolExecutor executor = (ThreadPoolExecutor) threadPool.getExecutor();
            FutureTask<?> f = (FutureTask<?>) executor.submit(() -> sa);
            sa.add(new FutureCompleterWithConfigurableInterrupt(f, shouldInterruptThread, executor));

            return sa;
        }

        @Override
        public Disposable schedule(Runnable action, long delayTime, TimeUnit unit) {
            throw new IllegalStateException("Hystrix does not support delayed scheduling");
        }
    }

    /**
     * Very similar to io.reactivex.internal.schedulers.ScheduledAction.FutureCompleter, but with configurable interrupt behavior
     */
    private static class FutureCompleterWithConfigurableInterrupt implements Disposable {
        private final FutureTask<?> f;
        private final Callable<Boolean> shouldInterruptThread;
        private final ThreadPoolExecutor executor;

        private FutureCompleterWithConfigurableInterrupt(FutureTask<?> f, Callable<Boolean> shouldInterruptThread, ThreadPoolExecutor executor) {
            this.f = f;
            this.shouldInterruptThread = shouldInterruptThread;
            this.executor = executor;
        }

        @Override
        public void dispose() {
            executor.remove(f);
            try {
                if (shouldInterruptThread.call()) {
                    f.cancel(true);
                } else {
                    f.cancel(false);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public boolean isDisposed() {
            return f.isCancelled();
        }
    }

}
