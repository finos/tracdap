/*
 * Copyright 2024 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.netty;

import io.netty.util.concurrent.DefaultEventExecutorChooserFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import org.finos.tracdap.common.exception.ETracInternal;


public class EventLoopScheduler implements EventExecutorChooserFactory {

    public static EventExecutorChooserFactory roundRobin() {
        return DefaultEventExecutorChooserFactory.INSTANCE;
    }

    public static EventExecutorChooserFactory preferLoopAffinity() {
        return new EventLoopScheduler(/* fallbackFactory = */ roundRobin());
    }

    public static EventExecutorChooserFactory requireLoopAffinity() {
        return new EventLoopScheduler(/* fallbackFactory = */ null);
    }

    private final EventExecutorChooserFactory fallbackFactory;

    private EventLoopScheduler(EventExecutorChooserFactory fallbackFactory) {
        this.fallbackFactory = fallbackFactory;
    }

    @Override
    public EventExecutorChooserFactory.EventExecutorChooser newChooser(EventExecutor[] eventExecutors) {

        var fallback = fallbackFactory != null
                ? fallbackFactory.newChooser(eventExecutors)
                : null;

        return new Chooser(eventExecutors, fallback);
    }

    private static class Chooser implements EventExecutorChooserFactory.EventExecutorChooser {

        private final EventExecutor[] eventExecutors;
        private final EventExecutorChooser fallback;

        Chooser(EventExecutor[] eventExecutors, EventExecutorChooser fallback) {
            this.eventExecutors = eventExecutors;
            this.fallback = fallback;
        }

        @Override
        public EventExecutor next() {

            for (var eventExecutor : eventExecutors)
                if (eventExecutor.inEventLoop())
                    return eventExecutor;

            if (fallback != null)
                return fallback.next();

            throw new ETracInternal("The current operation is running outside the registered event loop group");
        }
    }
}
