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

package com.accenture.trac.common.service;

import com.accenture.trac.common.config.ConfigBootstrap;
import com.accenture.trac.common.config.ConfigManager;
import com.accenture.trac.common.exception.EStartup;
import com.accenture.trac.common.util.VersionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.DefaultShutdownCallbackRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Timer;
import java.util.TimerTask;


public abstract class CommonServiceBase {

    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.of(10, ChronoUnit.SECONDS);
    private Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

    private final Logger log = LoggerFactory.getLogger(getClass());

    protected abstract void doStartUp() throws InterruptedException;
    protected abstract int doShutDown() throws InterruptedException;

    protected Duration getShutdownTimeout() {
        return shutdownTimeout;
    }

    protected void setShutdownTimeout(Duration shutdownTimeout) {
        this.shutdownTimeout = shutdownTimeout;
    }

    public void start() {

        // Do not register a shutdown hook unless explicitly requested
        start(false);
    }

    public void start(boolean registerShutdownHook) {

        try {
            var serviceClass = getClass();
            var componentName = VersionInfo.getComponentName(serviceClass);
            var componentVersion = VersionInfo.getComponentVersion(serviceClass);
            log.info("{} {}", componentName, componentVersion);
            log.info("Service is coming up...");

            doStartUp();

            // If requested, install a shutdown handler for a graceful exit
            // This is needed when running a real server instance, but not when running embedded tests
            if (registerShutdownHook) {
                var shutdownThread = new Thread(this::jvmShutdownHook, "shutdown");
                Runtime.getRuntime().addShutdownHook(shutdownThread);
            }

            // Keep the logging system active while shutdown hooks are running
            disableLog4jShutdownHook();

            log.info("Service is up and running");
        }
        catch (InterruptedException e) {

            log.error("Startup sequence was interrupted");
            Thread.currentThread().interrupt();
        }
        catch (RuntimeException e) {

            var errorMessage = "Service failed to start: " + e.getMessage();
            log.error(errorMessage, e);

            throw new EStartup(e.getMessage(), e);
        }
    }

    public int stop() {

        try {

            log.info("Service is going down...");

            var shutdownThread = Thread.currentThread();

            // Set up a timer to interrupt the shutdown sequence if it takes too long

            var interruptTask = new TimerTask() {
                @Override
                public void run() {
                    shutdownThread.interrupt();
                }
            };

            // Include an extra second grace period in the shutdown
            // This is to avoid triggering an extra interrupt if doShutDown uses the full shutdown window

            var shutdownMilliseconds = (shutdownTimeout.getSeconds() + 1) * 1000;

            var shutdownTimer = new Timer("shutdown_timeout", true);
            shutdownTimer.schedule(interruptTask, shutdownMilliseconds);

            // Run the service-specific shutdown code

            var exitCode = doShutDown();

            // Check to see whether the shutdown sequence was interrupted

            // Do not forcibly exit the JVM inside stop()
            // Exit code can be checked by embedded tests when the JVM will continue running

            if (shutdownThread.isInterrupted()) {
                log.error("Shutdown sequence was interrupted");
                return -1;
            }
            else if (exitCode != 0) {
                log.info("Service has gone down with errors");
                return exitCode;
            }
            else {
                log.info("Service has gone down cleanly");
                return exitCode;
            }
        }
        catch (InterruptedException e) {

            log.error("Shutdown sequence was interrupted");
            Thread.currentThread().interrupt();
            return -1;
        }
        catch (RuntimeException e) {

            var errorMessage = "Service did not stop cleanly: " + e.getMessage();
            log.error(errorMessage, e);
            return -1;
        }
        finally {

            // The logging system can be shut down now that the shutdown hook has completed
            explicitLog4jShutdown();
        }
    }

    private void disableLog4jShutdownHook() {

        // The default logging configuration disables logging in a shutdown hook
        // The logging system goes down when shutdown is initiated and messages in the shutdown sequence are lost
        // Removing the logging shutdown hook allows closing messages to go to the logs as normal

        // This is an internal API in Log4j, there is a config setting available
        // This approach means admins with custom logging configs don't need to know about shutdown hooks
        // Anyway we would need to use the internal API to explicitly close the context

        try {
            var logFactory = (Log4jContextFactory) LogManager.getFactory();
            ((DefaultShutdownCallbackRegistry) logFactory.getShutdownCallbackRegistry()).stop();
        }
        catch (Exception e) {

            // In case disabling the shutdown hook doesn't work, do not interrupt the startup sequence
            // As a backup, final shutdown messages are written to stdout / stderr

            log.warn("Logging shutdown hook is active (shutdown messages may be lost)");
        }
    }

    private void explicitLog4jShutdown() {

        // Since the logging shutdown hook is disabled, provide a way to explicitly shut down the logging system
        // Especially important for custom configurations connecting to external logging services or databases
        // In the event that disabling the shutdown hook did not work, this method will do nothing

        var logContext = LogManager.getContext();

        if (logContext instanceof LoggerContext)
            Configurator.shutdown((LoggerContext) logContext);
    }

    private void jvmShutdownHook() {

        log.info("Shutdown request received");

        var exitCode = this.stop();

        // Calling System.exit from inside a shutdown hook can lead to undefined behavior (often JVM hangs)
        // This is because it calls back into the shutdown handlers

        // Runtime.halt will stop the JVM immediately without calling back into shutdown hooks
        // At this point everything is either stopped or has failed to stop
        // So, it should be ok to use Runtime.halt and report the exit code

        Runtime.getRuntime().halt(exitCode);
    }

    public static void svcMain(Class<? extends CommonServiceBase> svcClass, String[] args) {

        try {

            var config = ConfigBootstrap.useCommandLine(svcClass, args);

            var constructor = svcClass.getConstructor(ConfigManager.class);
            var service = constructor.newInstance(config);

            service.start(true);
        }
        catch (EStartup e) {

            if (e.isQuiet())
                System.exit(e.getExitCode());

            System.err.println("The service failed to start: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(e.getExitCode());
        }
        catch (Exception e) {

            System.err.println("There was an unexpected error on the main thread: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(-1);
        }
    }
}
