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
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.function.Consumer;


public abstract class BaseProcessor<TSource, TTarget> implements Flow.Processor<TSource, TTarget> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Lock stateLock;
    private final Consumer<TSource> discardFunc;

    private ProcessorStatus status = ProcessorStatus.NOT_STARTED;
    private long nTargetRequested = 0;
    private long nTargetDelivered = 0;
    private long nSourceRequested = 0;
    private long nSourceDelivered = 0;

    private Flow.Subscription sourceSubscription;
    private Flow.Subscription targetSubscription;
    private Flow.Subscriber<? super TTarget> targetSubscriber;

    protected BaseProcessor(Consumer<TSource> discardFunc) {

        this.stateLock = new NullLock();
        this.discardFunc = discardFunc;
    }

    protected abstract void handleTargetSubscribe();
    protected abstract void handleTargetRequest();
    protected abstract void handleTargetCancel();
    protected abstract void handleSourceSubscribe();
    protected abstract void handleSourceNext(TSource item);
    protected abstract void handleSourceComplete();
    protected abstract void handleSourceError(Throwable error);
    protected abstract void close();

    protected final long nTargetRequested() { return nTargetRequested; }
    protected final long nTargetDelivered() { return nTargetDelivered; }
    protected final long nSourceRequested() { return nSourceRequested; }
    protected final long nSourceDelivered() { return nSourceDelivered; }


    @Override
    public final void subscribe(Flow.Subscriber<? super TTarget> subscriber) {

        try {

            var stateAccepted = processTargetSubscribe(subscriber);

            if (stateAccepted) {

                targetSubscriber.onSubscribe(targetSubscription);
                handleTargetSubscribe();
            }
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    private void targetRequest(long n) {

        try {

            var stateAccepted = processTargetRequest(n);

            if (stateAccepted)
                handleTargetRequest();
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    private void targetCancel() {

        try {

            var stateAccepted = processTargetCancel();

            if (stateAccepted) {
                handleTargetCancel();
                closeOnce();
            }
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    @Override
    public final void onSubscribe(Flow.Subscription subscription) {

        try {

            var stateAccepted = processSourceSubscribe(subscription);

            if (stateAccepted) {

                handleSourceSubscribe();

                if (nTargetRequested() > 0)
                    handleTargetRequest();
            }
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    @Override
    public final void onNext(TSource item) {

        var delivered = false;

        try {

            var stateAccepted = processSourceOnNext();

            if (stateAccepted) {
                delivered = true;
                handleSourceNext(item);
            }
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
        finally {

            if (!delivered && discardFunc != null)
                discardFunc.accept(item);
        }
    }

    @Override
    public final void onComplete() {

        try {

            var stateAccepted = processSourceOnComplete();

            if (stateAccepted)
                handleSourceComplete();
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    @Override
    public final void onError(Throwable error) {

        try {

            var stateAccepted = processSourceOnError();

            if (stateAccepted)
                handleSourceError(error);
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    protected final void doSourceRequest(long n) {

        try {

            var stateAccepted = processSourceRequest(n);

            if (stateAccepted)
                sourceSubscription.request(n);
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    final void doSourceCancel() {

        try {

            sourceSubscription.cancel();
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    final void doTargetNext(TTarget item) {

        try {

            nTargetDelivered += 1;
            targetSubscriber.onNext(item);
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    final void doTargetComplete() {

        try {

            var stateAccepted = processTargetComplete();

            if (stateAccepted) {
                targetSubscriber.onComplete();
                closeOnce();
            }
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    final void doTargetError(Throwable error) {

        try {

            var stateAccepted = processTargetError();

            if (stateAccepted) {
                targetSubscriber.onError(error);
                closeOnce();
            }
        }
        catch (Throwable e) {

            unexpectedError(e);
        }
    }

    private boolean processTargetSubscribe(Flow.Subscriber<? super TTarget> subscriber) {

        stateLock.lock();

        try {

            var priorStatus = status;

            switch (priorStatus) {

            case NOT_STARTED:
                status = ProcessorStatus.TARGET_SUBSCRIBED;
                targetSubscription = new Subscription();
                targetSubscriber = subscriber;
                return true;

            case SOURCE_SUBSCRIBED:
                status = ProcessorStatus.RUNNING;
                targetSubscription = new Subscription();
                targetSubscriber = subscriber;
                return true;

            default:
                status = ProcessorStatus.INVALID_STATE;
                throw new IllegalStateException();
            }
        }
        finally {

            stateLock.unlock();
        }
    }

    private boolean processTargetRequest(long n) {

        stateLock.lock();

        try {

            switch (status) {

            case RUNNING:
            case SOURCE_COMPLETE:
                nTargetRequested += n;
                return true;

            case TARGET_SUBSCRIBED:
                nTargetRequested += n;
                return false;

            // Already done, ignore request for more
            case COMPLETE:
            case FAILED_UPSTREAM:
            case FAILED:
            case CANCELLED:
                return false;

            default:
                throw new IllegalStateException();
            }
        }
        finally {

            stateLock.unlock();
        }
    }

    private boolean processTargetCancel() {

        stateLock.lock();

        try {

            var priorStatus = status;

            switch (priorStatus) {

            case RUNNING:
            case SOURCE_COMPLETE:
            case TARGET_SUBSCRIBED:
                status = ProcessorStatus.CANCELLED;
                return true;

            // Already done, ignore cancel request
            case COMPLETE:
            case FAILED_UPSTREAM:
            case FAILED:
            case CANCELLED:
                return false;

            default:
                status = ProcessorStatus.INVALID_STATE;
                throw new IllegalStateException();
            }
        }
        finally {

            stateLock.unlock();
        }
    }

    private boolean processSourceSubscribe(Flow.Subscription subscription) {

        stateLock.lock();

        try  {

            var priorStatus = status;

            switch (priorStatus) {

            case NOT_STARTED:
                status = ProcessorStatus.SOURCE_SUBSCRIBED;
                sourceSubscription = subscription;
                return true;

            case TARGET_SUBSCRIBED:
                status = ProcessorStatus.RUNNING;
                sourceSubscription = subscription;
                return true;

            default:
                status = ProcessorStatus.INVALID_STATE;
                throw new IllegalStateException();
            }
        }
        finally {

            stateLock.unlock();
        }
    }

    private boolean processSourceOnNext() {

        stateLock.lock();

        try  {

            switch (status) {

            case RUNNING:
                nSourceDelivered += 1;
                return true;

            // Already failed or cancelled, silently ignore onNext
            case FAILED_UPSTREAM:
            case FAILED:
            case CANCELLED:
                return false;

            default:
                status = ProcessorStatus.INVALID_STATE;
                throw new IllegalStateException();
            }
        }
        finally {

            stateLock.unlock();
        }
    }

    private boolean processSourceOnComplete() {

        stateLock.lock();

        try  {

            switch (status) {

            case RUNNING:
                status = ProcessorStatus.SOURCE_COMPLETE;
                return true;

            // Already failed or cancelled, silently ignore onComplete
            case FAILED_UPSTREAM:
            case FAILED:
            case CANCELLED:
                return false;

            default:
                status = ProcessorStatus.INVALID_STATE;
                throw new IllegalStateException();
            }
        }
        finally {

            stateLock.unlock();
        }
    }

    private boolean processSourceOnError() {

        stateLock.lock();

        try {

            switch (status) {

            case RUNNING:
                status = ProcessorStatus.FAILED_UPSTREAM;
                return true;

            // Already failed or cancelled, do not report a second error
            case FAILED_UPSTREAM:
            case FAILED:
            case CANCELLED:
                return false;

            default:
                status = ProcessorStatus.INVALID_STATE;
                throw new IllegalStateException();
            }
        }
        finally {

            stateLock.unlock();
        }
    }

    private boolean processSourceRequest(long n) {

        stateLock.lock();

        try  {

            if (status == ProcessorStatus.RUNNING) {

                nSourceRequested += n;
                return true;
            }
            else {

                status = ProcessorStatus.INVALID_STATE;
                throw new IllegalStateException();
            }
        }
        finally {

            stateLock.unlock();
        }
    }

    private boolean processTargetComplete() {

        stateLock.lock();

        try {

            if (status != ProcessorStatus.SOURCE_COMPLETE) {
                status = ProcessorStatus.INVALID_STATE;
                throw new IllegalStateException();
            }

            status = ProcessorStatus.COMPLETE;
            return true;
        }
        finally {

            stateLock.unlock();
        }
    }

    private boolean processTargetError() {

        stateLock.lock();

        try {

            var priorStatus = status;

            switch (priorStatus) {

            case TARGET_SUBSCRIBED:
            case RUNNING:
            case SOURCE_COMPLETE:
            case FAILED_UPSTREAM:

                status = ProcessorStatus.FAILED;
                return true;

            case CANCELLED:
            case FAILED:
            case COMPLETE:
                return false;

            default:
                status = ProcessorStatus.INVALID_STATE;
                throw new IllegalStateException();
            }
        }
        finally {

            stateLock.unlock();
        }
    }

    private void closeOnce() {

        var notAlreadyClosed = closed.compareAndSet(false, true);

        if (notAlreadyClosed)
            close();
    }

    private void unexpectedError(Throwable error) {

        stateLock.lock();
        boolean alreadyDone = false;
        boolean gotSource = false;
        boolean gotTarget = false;

        try {

            var priorStatus = status;

            switch (priorStatus) {

            case CANCELLED:
            case FAILED:
            case COMPLETE:
                alreadyDone = true;
                gotSource = true;
                gotTarget = true;
                break;

            default:

                if (sourceSubscription != null)
                    gotSource = true;

                if (targetSubscriber != null) {
                    gotTarget = true;
                    status = ProcessorStatus.FAILED;
                }
                else
                    status = ProcessorStatus.INVALID_STATE;
            }
        }
        finally {

            stateLock.unlock();
        }

        try {

            if (alreadyDone)
                log.warn("An unexpected error occurred after processing is complete: " + error.getMessage(), error);

            else if (!gotSource || !gotTarget)
                log.error("An unexpected error occurred before processing began: " + error.getMessage(), error);

            else
                log.error("An unexpected error occurred during processing: " + error.getMessage(), error);


            if (gotTarget && !alreadyDone)
                targetSubscriber.onError(error);

            if (gotSource && !alreadyDone)
                sourceSubscription.cancel();
        }
        finally {

            closeOnce();
        }
    }

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

    private static class NullLock implements Lock {

        @Override public void lock() {}
        @Override public void lockInterruptibly() {}
        @Override public boolean tryLock() { return true; }
        @Override public boolean tryLock(long time, @Nonnull TimeUnit unit) { return true; }
        @Override public void unlock() {}
        @Override @Nonnull public Condition newCondition() { throw new UnsupportedOperationException(); }
    }
}
