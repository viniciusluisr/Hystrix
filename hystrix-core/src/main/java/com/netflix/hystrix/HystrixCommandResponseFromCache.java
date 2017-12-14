package com.netflix.hystrix;

import io.reactivex.Observable;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;

import java.util.concurrent.atomic.AtomicBoolean;

public class HystrixCommandResponseFromCache<R> extends HystrixCachedObservable<R> {
    private final AbstractCommand<R> originalCommand;

    /* package-private */ HystrixCommandResponseFromCache(Observable<R> originalObservable, final AbstractCommand<R> originalCommand) {
        super(originalObservable);
        this.originalCommand = originalCommand;
    }

    public Observable<R> toObservableWithStateCopiedInto(final AbstractCommand<R> commandToCopyStateInto) {
        final AtomicBoolean completionLogicRun = new AtomicBoolean(false);

        return cachedObservable
                .doOnError(new Consumer<Throwable>() {
                    @Override
                    public void accept(Throwable throwable) {
                        if (completionLogicRun.compareAndSet(false, true)) {
                            commandCompleted(commandToCopyStateInto);
                        }
                    }
                })
                .doOnComplete(new Action() {
                    @Override
                    public void run() {
                        if (completionLogicRun.compareAndSet(false, true)) {
                            commandCompleted(commandToCopyStateInto);
                        }
                    }
                })
                .doOnDispose(new Action() {
                    @Override
                    public void run() {
                        if (completionLogicRun.compareAndSet(false, true)) {
                            commandUnsubscribed(commandToCopyStateInto);
                        }
                    }
                });
    }

    private void commandCompleted(final AbstractCommand<R> commandToCopyStateInto) {
        commandToCopyStateInto.executionResult = originalCommand.executionResult;
    }

    private void commandUnsubscribed(final AbstractCommand<R> commandToCopyStateInto) {
        commandToCopyStateInto.executionResult = commandToCopyStateInto.executionResult.addEvent(HystrixEventType.CANCELLED);
        commandToCopyStateInto.executionResult = commandToCopyStateInto.executionResult.setExecutionLatency(-1);
    }
}
