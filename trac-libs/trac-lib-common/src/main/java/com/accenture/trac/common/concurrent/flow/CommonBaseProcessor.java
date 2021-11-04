/*
 * Copyright 2021 Accenture Global Solutions Limited
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

package com.accenture.trac.common.concurrent.flow;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;


public abstract class CommonBaseProcessor <TSource, TTarget> implements Flow.Processor<TSource, TTarget> {

    protected enum ProcessorStatus {
        NOT_STARTED,
        SOURCE_SUBSCRIBED,
        TARGET_SUBSCRIBED,
        RUNNING,
        SOURCE_COMPLETE,
        COMPLETE,
        FAILED_UPSTREAM,
        FAILED_INTERNAL,
        CANCELLED,
        INVALID_STATE
    }

    private ProcessorStatus status = ProcessorStatus.NOT_STARTED;
    private long nTargetRequested = 0;
    private long nTargetDelivered = 0;
    private long nSourceRequested = 0;
    private long nSourceDelivered = 0;

    private Flow.Subscription sourceSubscription;
    private Flow.Subscription targetSubscription;
    private Flow.Subscriber<? super TTarget> targetSubscriber;

    private final Lock stateLock;
    private final LockKeeper stateLockKeeper;

    protected CommonBaseProcessor() {

        stateLock = new NullLock();
        stateLockKeeper = new LockKeeper();
    }

    @Override
    public final void subscribe(Flow.Subscriber<? super TTarget> subscriber) {

        try (var lock = reusableLock()) {

            lock.acquire();

            var priorStatus = status;

            switch (priorStatus) {

                case NOT_STARTED:
                    status = ProcessorStatus.TARGET_SUBSCRIBED;
                    break;

                case SOURCE_SUBSCRIBED:
                    status = ProcessorStatus.RUNNING;
                    break;

                default:
                    status = ProcessorStatus.INVALID_STATE;
                    throw new IllegalStateException();
            }

            targetSubscription = new Subscription();
            targetSubscriber = subscriber;
        }

        handleTargetSubscribe();

        targetSubscriber.onSubscribe(targetSubscription);
    }

    private void targetRequest(long n) {

        boolean doRequest = false;

        try (var lock = reusableLock()) {

            lock.acquire();

            switch (status) {

                case RUNNING:
                    nTargetRequested += n;
                    doRequest = true;
                    break;

                case SOURCE_COMPLETE:
                case TARGET_SUBSCRIBED:
                    nTargetRequested += n;
                    break;

                case COMPLETE:
                case FAILED_UPSTREAM:
                case FAILED_INTERNAL:
                case CANCELLED:
                    break;

                default:
                    throw new IllegalStateException();
            }
        }

        if (doRequest)
            handleTargetRequest();
    }

    private void targetCancel() {

        boolean doUpstreamCancel = false;

        try (var lock = reusableLock()) {

            lock.acquire();

            var priorStatus = status;

            switch (priorStatus) {

                case RUNNING:
                    doUpstreamCancel = true;
                    status = ProcessorStatus.CANCELLED;
                    break;

                case SOURCE_COMPLETE:
                case TARGET_SUBSCRIBED:
                    status = ProcessorStatus.CANCELLED;
                    break;

                case COMPLETE:
                case FAILED_UPSTREAM:
                case FAILED_INTERNAL:
                case CANCELLED:
                    break;

                default:
                    throw new IllegalStateException();
            }
        }

        if (doUpstreamCancel)
            sourceSubscription.cancel();
    }

    @Override
    public final void onSubscribe(Flow.Subscription subscription) {

        boolean doRequest;

        try (var lock = reusableLock()) {

            lock.acquire();

            var priorStatus = status;

            switch (priorStatus) {

                case NOT_STARTED:
                    status = ProcessorStatus.SOURCE_SUBSCRIBED;
                    break;

                case TARGET_SUBSCRIBED:
                    status = ProcessorStatus.RUNNING;
                    break;

                default:
                    status = ProcessorStatus.INVALID_STATE;
                    throw new IllegalStateException();
            }

            sourceSubscription = subscription;
            doRequest = (nTargetRequested > 0);
        }

        handleSourceSubscribe();

        if (doRequest)
            handleTargetRequest();
    }

    @Override
    public final void onNext(TSource item) {

        nSourceDelivered += 1;

        handleSourceNext(item);
    }

    @Override
    public final void onError(Throwable error) {

        handleSourceError(error);
    }

    @Override
    public final void onComplete() {

        handleSourceComplete();
    }

    private class Subscription implements Flow.Subscription {

        @Override
        public void request(long n) {
            targetRequest(n);
        }

        @Override
        public void cancel() {
            targetCancel();
        }
    }




    protected void handleTargetSubscribe() {

    }

    protected void handleTargetRequest() {

        if (nTargetRequested > nTargetDelivered && nSourceRequested <= nSourceDelivered)
            doSourceRequest(nTargetRequested - nTargetDelivered);
    }

    protected void handleTargetCancel() {

        doSourceCancel();
    }

    protected void handleSourceSubscribe() {

    }

    protected abstract void handleSourceNext(TSource item);

    protected abstract void handleSourceComplete();

    protected void handleSourceError(Throwable error) {

        var completionError = error instanceof CompletionException
                ? error
                : new CompletionException(error.getMessage(), error);

        doTargetError(completionError);
    }

    protected final void doSourceRequest(long n) {

        nSourceRequested += n;
        sourceSubscription.request(n);
    }

    protected final void doSourceCancel() {

        sourceSubscription.cancel();
    }

    protected final void doTargetNext(TTarget item) {

        nTargetDelivered += 1;
        targetSubscriber.onNext(item);
    }

    protected final void doTargetComplete() {

        // TODO: State

        targetSubscriber.onComplete();
    }

    protected final void doTargetError(Throwable error) {

        targetSubscriber.onError(error);
    }


    protected final long nTargetRequested() { return nTargetRequested; }
    protected final long nTargetDelivered() { return nTargetDelivered; }
    protected final long nSourceRequested() { return nSourceRequested; }
    protected final long nSourceDelivered() { return nSourceDelivered; }






    private LockKeeper reusableLock() {
        return stateLockKeeper;
    }

    private class LockKeeper implements AutoCloseable {

        public void acquire() {
            stateLock.lock();
        }

        @Override
        public void close() {
            stateLock.unlock();
        }
    }

    private static class NullLock implements Lock {

        @Override public void lock() {}
        @Override public void lockInterruptibly() {}
        @Override public boolean tryLock() { return true; }
        @Override public boolean tryLock(long time, @Nonnull TimeUnit unit) { return true; }
        @Override public void unlock() {}
        @Override @Nonnull public Condition newCondition() { throw new UnsupportedOperationException(); }
    }
}
