/*
 * Licensed to the Fintech Open Source Foundation (FINOS) under one or
 * more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * FINOS licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.finos.tracdap.svc.orch.jobs;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;


/**
 * Capture the ERROR events every logger in this JVM writes, while the capture is open.
 * Each service start reconfigures logging, so attach a capture after the services have started.
 */
public class ErrorLogCapture implements AutoCloseable {

    private final LoggerContext context;
    private final AbstractAppender appender;
    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    public static ErrorLogCapture attach() {
        return new ErrorLogCapture();
    }

    private ErrorLogCapture() {

        context = (LoggerContext) LogManager.getContext(false);
        appender = new AbstractAppender("error-capture-" + UUID.randomUUID(), null, null, true, Property.EMPTY_ARRAY) {

            @Override
            public void append(LogEvent event) {
                if (event.getLevel().isMoreSpecificThan(Level.ERROR))
                    events.add(event.toImmutable());
            }
        };

        appender.start();
        context.getConfiguration().getRootLogger().addAppender(appender, Level.ERROR, null);
        context.updateLoggers();
    }

    /** Errors whose message or stack trace mentions the given text */
    public List<String> errorsMentioning(String text) {

        return events.stream()
                .map(ErrorLogCapture::describe)
                .filter(description -> description.contains(text))
                .collect(Collectors.toList());
    }

    private static String describe(LogEvent event) {

        var description = new StringBuilder(event.getLoggerName())
                .append(": ")
                .append(event.getMessage().getFormattedMessage());

        for (var error = event.getThrown(); error != null; error = error.getCause())
            description.append(" | ").append(error);

        return description.toString();
    }

    @Override
    public void close() {

        context.getConfiguration().getRootLogger().removeAppender(appender.getName());
        context.updateLoggers();
        appender.stop();
    }
}
