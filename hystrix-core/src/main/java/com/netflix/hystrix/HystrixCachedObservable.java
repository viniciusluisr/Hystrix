package com.netflix.hystrix;

import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Action;
import io.reactivex.subjects.ReplaySubject;
import org.reactivestreams.Subscription;

public class HystrixCachedObservable<R> {
    protected final Subscription originalSubscription;
    protected final Observable<R> cachedObservable;
    private volatile int outstandingSubscriptions = 0;

    protected HystrixCachedObservable(final Observable<R> originalObservable) {
        ReplaySubject<R> replaySubject = ReplaySubject.create();
        this.originalSubscription = originalObservable
                .subscribe(replaySubject);

        this.cachedObservable = replaySubject
                .doOnDispose(new Action() {
                    @Override
                    public void run() {
                        outstandingSubscriptions--;
                        if (outstandingSubscriptions == 0) {
                            originalSubscription.unsubscribe();
                        }
                    }
                })
                .doOnSubscribe(new Action() {
                    @Override
                    public void run() {
                        outstandingSubscriptions++;
                    }
                });
    }

    public static <R> HystrixCachedObservable<R> from(Observable<R> o, AbstractCommand<R> originalCommand) {
        return new HystrixCommandResponseFromCache<R>(o, originalCommand);
    }

    public static <R> HystrixCachedObservable<R> from(Observable<R> o) {
        return new HystrixCachedObservable<R>(o);
    }

    public Observable<R> toObservable() {
        return cachedObservable;
    }

    public void unsubscribe() {
        originalSubscription.unsubscribe();
    }
}
