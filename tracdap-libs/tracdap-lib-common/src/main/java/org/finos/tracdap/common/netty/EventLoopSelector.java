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


public class EventLoopSelector implements EventExecutorChooserFactory {

    private final EventExecutorChooserFactory fallbackFactory;

    public EventLoopSelector() {
        fallbackFactory = DefaultEventExecutorChooserFactory.INSTANCE;
    }

    @Override
    public EventExecutorChooserFactory.EventExecutorChooser newChooser(EventExecutor[] eventExecutors) {

        var fallback = fallbackFactory.newChooser(eventExecutors);

        return new Chooser(eventExecutors, fallback);
    }

    public static class Chooser implements EventExecutorChooserFactory.EventExecutorChooser {

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

            return fallback.next();
        }
    }
}
