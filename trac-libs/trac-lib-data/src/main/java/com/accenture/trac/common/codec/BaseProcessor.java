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

package com.accenture.trac.common.codec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;


public abstract class BaseProcessor<TSource, TTarget> implements Flow.Processor<TSource, TTarget> {

    protected enum ProcessorStatus {
        NOT_STARTED,
        SOURCE_SUBSCRIBED,
        TARGET_SUBSCRIBED,
        RUNNING,
        SOURCE_COMPLETE,
        COMPLETE,
        FAILED_UPSTREAM,
        FAILED,
        CANCELLED,
        INVALID_STATE
    }

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Lock stateLock;
    private final LockKeeper stateLockKeeper;
    private final Consumer<TSource> discardFunc;

    private ProcessorStatus status = ProcessorStatus.NOT_STARTED;
    private long nTargetRequested = 0;
    private long nTargetDelivered = 0;
    private long nSourceRequested = 0;
    private long nSourceDelivered = 0;

    private Flow.Subscription sourceSubscription;
    private Flow.Subscription targetSubscription;
    private Flow.Subscriber<? super TTarget> targetSubscriber;

    protected BaseProcessor() {

        this.stateLock = new NullLock();
        this.stateLockKeeper = new LockKeeper();
        this.discardFunc = null;
    }

    protected BaseProcessor(Consumer<TSource> discardFunc) {

        this.stateLock = new NullLock();
        this.stateLockKeeper = new LockKeeper();
        this.discardFunc = discardFunc;
    }

    protected final ProcessorStatus status() { return status; }
    protected final long nTargetRequested() { return nTargetRequested; }
    protected final long nTargetDelivered() { return nTargetDelivered; }
    protected final long nSourceRequested() { return nSourceRequested; }
    protected final long nSourceDelivered() { return nSourceDelivered; }


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
                case SOURCE_COMPLETE:
                    nTargetRequested += n;
                    doRequest = true;
                    break;

                case TARGET_SUBSCRIBED:
                    nTargetRequested += n;
                    break;

                // Already done, ignore request for more
                case COMPLETE:
                case FAILED_UPSTREAM:
                case FAILED:
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

                // Already done, ignore cancel request
                case COMPLETE:
                case FAILED_UPSTREAM:
                case FAILED:
                case CANCELLED:
                    break;

                default:
                    status = ProcessorStatus.INVALID_STATE;
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

        var doDeliver = false;
        var doDiscard = false;

        try (var lock = reusableLock()) {

            lock.acquire();

            switch (status) {

                case RUNNING:
                    doDeliver = true;
                    nSourceDelivered += 1;
                    break;

                // Already failed or cancelled, silently ignore onNext
                case FAILED_UPSTREAM:
                case FAILED:
                case CANCELLED:
                    doDiscard = true;
                    break;

                default:
                    doDiscard = true;
                    status = ProcessorStatus.INVALID_STATE;
                    throw new IllegalStateException();
            }
        }
        finally {

            if (doDiscard && discardFunc != null)
                discardFunc.accept(item);
        }

        if (doDeliver)
            handleSourceNext(item);
    }

    @Override
    public final void onComplete() {

        var doReportComplete = false;

        try (var lock = reusableLock()) {

            lock.acquire();

            switch (status) {

                case RUNNING:
                    doReportComplete = true;
                    status = ProcessorStatus.SOURCE_COMPLETE;
                    break;

                // Already failed or cancelled, silently ignore onComplete
                case FAILED_UPSTREAM:
                case FAILED:
                case CANCELLED:
                    break;

                default:
                    status = ProcessorStatus.INVALID_STATE;
                    throw new IllegalStateException();
            }
        }

        // todo

        try {

            if (doReportComplete)
                handleSourceComplete();
        }
        catch (Throwable e) {

            doTargetError(e);
        }
    }

    @Override
    public final void onError(Throwable error) {

        var doReportError = false;

        try (var lock = reusableLock()) {

            lock.acquire();

            switch (status) {

                case RUNNING:
                    doReportError = true;
                    status = ProcessorStatus.FAILED_UPSTREAM;
                    break;

                // Already failed or cancelled, do not report a second error
                case FAILED_UPSTREAM:
                case FAILED:
                case CANCELLED:
                    break;

                default:
                    status = ProcessorStatus.INVALID_STATE;
                    throw new IllegalStateException();
            }
        }

        if (doReportError)
            handleSourceError(error);
    }

    final void doSourceRequest(long n) {

        var doRequest = false;

        try (var lock = reusableLock()) {

            lock.acquire();

            switch (status) {

                case RUNNING:
                    nSourceRequested += n;
                    doRequest = true;
                    break;

                default:
                    status = ProcessorStatus.INVALID_STATE;
                    throw new IllegalStateException();
            }
        }

        if (doRequest)
            sourceSubscription.request(n);
    }

    final void doSourceCancel() {

        sourceSubscription.cancel();
    }

    final void doTargetNext(TTarget item) {

        nTargetDelivered += 1;
        targetSubscriber.onNext(item);
    }

    final void doTargetComplete() {

        try (var lock = reusableLock()) {

            lock.acquire();

            if (status != ProcessorStatus.SOURCE_COMPLETE) {
                status = ProcessorStatus.INVALID_STATE;
                throw new IllegalStateException();
            }

            status = ProcessorStatus.COMPLETE;
        }

        targetSubscriber.onComplete();
    }

    final void doTargetError(Throwable error) {

        var doReportError = false;

        try (var lock = reusableLock()) {

            lock.acquire();

            var priorStatus = status;

            switch (priorStatus) {

                case TARGET_SUBSCRIBED:
                case RUNNING:
                case SOURCE_COMPLETE:
                case FAILED_UPSTREAM:

                    status = ProcessorStatus.FAILED;
                    doReportError = true;
                    break;

                case CANCELLED:
                case FAILED:
                case COMPLETE:

                    log.warn("An error occurred after processing is complete: " + error.getMessage(), error);
                    break;

                default:
                    status = ProcessorStatus.INVALID_STATE;
                    throw new IllegalStateException();
            }
        }

        try {

            if (doReportError)
                targetSubscriber.onError(error);
        }
        catch (Throwable secondaryError) {

            log.warn("Failed to report an error, this is likely to cause hangs or resource leaks");
            log.warn("Original error: {}", error.getMessage(), error);
            log.warn("Secondary error: {}", secondaryError.getMessage(), error);
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
