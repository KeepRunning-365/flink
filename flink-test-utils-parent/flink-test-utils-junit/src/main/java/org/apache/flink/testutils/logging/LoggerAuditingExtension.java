/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.testutils.logging;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

/**
 * Utility for auditing logged messages.(Junit5 extension)
 *
 * <p>Implementation note: Make sure to not expose log4j dependencies in the interface of this class
 * to ease updates in logging infrastructure.
 */
public class LoggerAuditingExtension implements BeforeEachCallback, AfterEachCallback {
    private static final LoggerContext LOGGER_CONTEXT =
            (LoggerContext) LogManager.getContext(false);

    private final String loggerName;
    private final org.slf4j.event.Level level;

    private ConcurrentLinkedQueue<LogEvent> loggingEvents;

    public LoggerAuditingExtension(Class<?> clazz, org.slf4j.event.Level level) {
        this(clazz.getCanonicalName(), level);
    }

    public LoggerAuditingExtension(String loggerName, org.slf4j.event.Level level) {
        this.loggerName = loggerName;
        this.level = level;
    }

    public List<String> getMessages() {
        return loggingEvents.stream()
                .map(e -> e.getMessage().getFormattedMessage())
                .collect(Collectors.toList());
    }

    public List<LogEvent> getEvents() {
        return new ArrayList<>(loggingEvents);
    }

    public String getLoggerName() {
        return loggerName;
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        loggingEvents = new ConcurrentLinkedQueue<>();

        Appender testAppender =
                new AbstractAppender("test-appender", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        loggingEvents.add(event.toImmutable());
                    }
                };
        testAppender.start();

        AppenderRef appenderRef = AppenderRef.createAppenderRef(testAppender.getName(), null, null);
        LoggerConfig logger =
                LoggerConfig.createLogger(
                        false,
                        Level.getLevel(level.name()),
                        "test",
                        null,
                        new AppenderRef[] {appenderRef},
                        null,
                        LOGGER_CONTEXT.getConfiguration(),
                        null);
        logger.addAppender(testAppender, null, null);

        LOGGER_CONTEXT.getConfiguration().addLogger(loggerName, logger);
        LOGGER_CONTEXT.updateLoggers();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        LOGGER_CONTEXT.getConfiguration().removeLogger(loggerName);
        LOGGER_CONTEXT.updateLoggers();
        loggingEvents = null;
    }
}
