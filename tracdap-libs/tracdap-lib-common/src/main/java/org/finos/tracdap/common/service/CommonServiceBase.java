/*
 * Copyright 2022 Accenture Global Solutions Limited
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

package org.finos.tracdap.common.service;

import org.finos.tracdap.common.plugin.PluginManager;
import org.finos.tracdap.common.startup.Startup;
import org.finos.tracdap.common.config.ConfigManager;
import org.finos.tracdap.common.exception.EStartup;
import org.finos.tracdap.common.exception.ETrac;
import org.finos.tracdap.common.util.VersionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.DefaultShutdownCallbackRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Timer;
import java.util.TimerTask;


/**
 * Base class for implementing TRAC services
 *
 * <p>The service base class supplies start/stop control, signal handling and timeouts.
 * It also calls ConfigBootstrap and supplies a config to the service class.</p>
 *
 * <p>Services implement doStartup() and doShutdown to provide startup and shutdown
 * sequences. The base class will take care of registering signal hooks to call these
 * methods when needed. The base class also provides timeouts for the start and stop
 * sequences, so they will automatically fail if they do not complete in the required
 * time.</p>
 */
public abstract class CommonServiceBase {

    private static final Duration DEFAULT_STARTUP_TIMEOUT = Duration.of(30, ChronoUnit.SECONDS);
    private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.of(30, ChronoUnit.SECONDS);


    // -----------------------------------------------------------------------------------------------------------------
    // SERVICE INTERFACE
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Services must implement this method to provide their startup logic
     *
     * <p>When implementing doStartup, services should wait for all resources to be brought
     * up. Where resources are created asynchronously, use await() or sync() or equivalent
     * calls to wait for those operations to complete and check the results. In the case of
     * errors, doStartup should throw an exception (preferably EStartup). There is no
     * mechanism of callbacks to report startup results after doStartup returns.</p>
     *
     * <p>The startup sequence must complete within the specified timeout. Services can
     * use the timeout parameter to e.g. set timeouts in await() or equivalent calls.
     * Alternatively, the service does not need to set timeouts on startup operations, and
     * can instead rely on the base class to interrupt the startup thread if the timeout expires.
     * To change the startup timeout, use setStartupTimeout() before the startup sequence begins,
     * i.e. in the constructor (it is not possible to change the timeout once doStartup has been called).
     * The default startup timeout is 30 seconds.</p>
     *
     * <p>Services should ensure there is at least one service thread (i.e. non-daemon thread)
     * created by the startup process as the main thread will terminate once the startup
     * sequence is complete.</p>
     *
     * @param startupTimeout The maximum time allowed before the startup sequence is considered to have failed
     * @throws InterruptedException The startup sequence is interrupted, either externally or by a timeout
     * @throws EStartup The startup sequence should throw EStartup if it fails for any reason
     */
    protected abstract void doStartup(Duration startupTimeout) throws InterruptedException;

    /**
     * Services must implement this method to provide their shutdown logic
     *
     * <p>Shutdown all resources and return an exit code, to be used as the exit code of the process.
     * When implementing doShutdown, services should wait for all resources to be brought
     * down. Where resources are destroyed asynchronously, use await() or sync() or equivalent
     * calls to wait for those operations to complete and check the results. In the case of
     * errors, doShutdown should return a non-zero result (or throw an error). Only if all
     * resources are closed successfully should doShutdown return a zero result.</p>
     *
     * <p>The shutdown sequence must complete within the specified timeout. Services can
     * use the timeout parameter to e.g. set timeouts in await() or equivalent calls.
     * Alternatively, the service does not need to set timeouts on shutdown operations, and
     * can instead rely on the base class to interrupt the shutdown thread if the timeout expires.
     * To change the shutdown timeout, use setShutdownTimeout() before the shutdown sequence begins,
     * e.g. during startup when config is loaded (it is not possible to change the timeout once
     * doShutdown has been called). The default shutdown timeout is 30 seconds.</p>
     *
     * @param shutdownTimeout The maximum time allowed before the shutdown sequence is considered to have failed
     * @return The process exit code, that will be passed back to the OS when the process exits
     * @throws InterruptedException The shutdown sequence is interrupted, either externally or by a timeout
     * @throws ETrac The shutdown sequence can throw an error from the ETrac hierarchy if it fails for any reason
     */
    protected abstract int doShutdown(Duration shutdownTimeout) throws InterruptedException;

    /**
     * Helper function for shutting down resources with a shutdown deadline
     *
     * @param resourceName Name of the resource being shut down (will appear in the logs)
     * @param deadline Shutdown deadline
     * @param action An action to shut down the resource, returns a boolean value indicating success/failure
     * @return The success value returned by the shutdown action
     */
    protected boolean shutdownResource(String resourceName, Instant deadline, ShutdownAction action) {

        var remaining = Duration.between(Instant.now(), deadline);
        var safeRemaining = remaining.isNegative() ? Duration.ZERO : remaining;

        boolean ok;

        try {
            ok = action.apply(safeRemaining);
        }
        catch (InterruptedException e) {
            ok = false;
            Thread.currentThread().interrupt();
        }

        if (ok) {
            log.info("{} has gone down", resourceName);
        }
        else {
            log.error("{} did not go down cleanly", resourceName);
        }

        return ok;
    }

    @FunctionalInterface
    protected interface ShutdownAction {

        boolean apply(Duration remainingTime) throws InterruptedException;
    }


    // -----------------------------------------------------------------------------------------------------------------
    // IMPLEMENTATION
    // -----------------------------------------------------------------------------------------------------------------

    private final Logger log = LoggerFactory.getLogger(getClass());

    // At present no services are changing the startup / shutdown timeout
    // If necessary, we can add setStartupTimeout and / or setShutdownTimeout to change these values

    private final Duration startupTimeout = DEFAULT_STARTUP_TIMEOUT;
    private final Duration shutdownTimeout = DEFAULT_SHUTDOWN_TIMEOUT;

    /**
     * Entry point for spawning a new service
     *
     * <p>Services can call this method from their own main() method to get the standard startup sequence.</p>
     *
     * @param svcClass The service class to be spawned
     * @param args Command line args passed into the JVM
     */
    public static void svcMain(Class<? extends CommonServiceBase> svcClass, String[] args) {

        try {

            var startup = Startup.useCommandLine(svcClass, args);
            startup.runStartupSequence();

            var plugins = startup.getPlugins();
            var config = startup.getConfig();

            var constructor = svcClass.getConstructor(PluginManager.class, ConfigManager.class);
            var service = constructor.newInstance(plugins, config);

            service.start(true);

            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException();
        }
        catch (EStartup e) {

            if (e.isQuiet())
                System.exit(e.getExitCode());

            System.err.println("Service failed to start: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(e.getExitCode());
        }
        catch (ETrac e) {

            System.err.println("Service failed to start: " + e.getMessage());
            e.printStackTrace(System.err);

            System.exit(-1);
        }
        catch (InterruptedException e) {

            System.err.println("Service failed to start: Startup sequence was interrupted");
            Thread.currentThread().interrupt();

            System.exit(-2);
        }
        catch (NoSuchMethodException e) {

            System.err.println("Service failed to start: Missing required service constructor for svcMain (this is a bug)");
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);

            System.exit(-3);
        }
        catch (Exception e) {

            System.err.println("Service failed to start: There was an unhandled error during startup (this is a bug)");
            System.err.println(e.getMessage());
            e.printStackTrace(System.err);

            System.exit(-3);
        }
    }

    /**
     * Start the service
     *
     * <p>This method does not register any shutdown hooks and so is suitable for
     * embedded services and for testing.</p>
     *
     * <p>Note: Services started using svcMain() have their lifecycle managed automatically,
     * there is no need to call start() or stop().</P>
     */
    public void start() {

        // Do not register a shutdown hook unless explicitly requested
        start(false);
    }

    /**
     * Start the service
     *
     * <P>This method can optionally register shutdown hooks, which are needed to
     * run a standalone instance of the service.</P>
     *
     * <P>Note: Services started using svcMain() have their lifecycle managed automatically,
     * there is no need to call start() or stop().</P>
     *
     * @param registerShutdownHook Flag indicating a JVM shutdown hook should be registered to manage clean shutdowns
     */
    public void start(boolean registerShutdownHook) {

        try {
            var serviceClass = getClass();
            var componentName = VersionInfo.getComponentName(serviceClass);
            var componentVersion = VersionInfo.getComponentVersion(serviceClass);
            log.info("{} {}", componentName, componentVersion);
            log.info("Service is coming up...");

            timedSequence(t -> {doStartup(t); return null;}, startupTimeout, "startup");

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
        catch (ETrac e) {

            // Do not log errors for quiet startup exceptions
            if (e instanceof EStartup && ((EStartup) e).isQuiet())
                throw e;

            var errorMessage = "Service failed to start: " + e.getMessage();
            log.error(errorMessage, e);
            throw e;
        }
        catch (InterruptedException e) {

            log.error("Service failed to start: Startup sequence was interrupted");
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {

            log.error("Service failed to start: There was an unhandled error during startup (this is a bug)");
            log.error(e.getMessage(), e);

            throw new EStartup(e.getMessage(), e);
        }
    }

    /**
     * Stop the service
     *
     * <P>This method can be used to stop both standalone services and embedded/testing instances.
     * Returns zero on success, non-zero on failure.</P>
     *
     * @return A result code that can be used as the exit code of the Java process
     */
    public int stop() {

        try {

            log.info("Service is going down...");

            var exitCode = timedSequence(this::doShutdown, shutdownTimeout, "shutdown");

            // Do not forcibly exit the JVM inside stop()
            // Exit code can be checked by embedded tests when the JVM will continue running

            if (exitCode == 0)
                log.info("Service has gone down cleanly");

            else
                log.error("Service has gone down with errors");

            return exitCode;
        }
        catch (ETrac e) {

            var errorMessage = "Service did not stop cleanly: " + e.getMessage();
            log.error(errorMessage, e);
            return -1;
        }
        catch (InterruptedException e) {

            log.error("Service did not stop cleanly: Shutdown sequence was interrupted");
            Thread.currentThread().interrupt();
            return -2;
        }
        catch (Exception e) {

            log.error("Service did not stop cleanly: There was an unhandled error during shutdown (this is a bug)");
            log.error(e.getMessage(), e);
            return -3;
        }
        finally {

            // The logging system can be shut down now that the shutdown hook has completed
            explicitLog4jShutdown();
        }
    }

    @FunctionalInterface
    private interface TimedSequence<TResult> {

        TResult run(Duration timeout) throws InterruptedException;
    }

    private <TResult>
    TResult timedSequence(
            TimedSequence<TResult> sequence,
            Duration timeout,
            String sequenceName)
            throws InterruptedException {

        // Set up a timer to interrupt the shutdown sequence if it takes too long

        var thread = Thread.currentThread();

        var interruptTask = new TimerTask() {
            @Override
            public void run() {
                log.error("Timeout expired for {} sequence ({} seconds)", sequenceName, timeout.getSeconds());
                thread.interrupt();
            }
        };

        // Include an extra second grace period in the shutdown
        // This is to avoid triggering an extra interrupt if doShutDown uses the full shutdown window

        var timeoutMillis = (timeout.getSeconds() + 1) * 1000;

        var timer = new Timer(sequenceName + "_timer", true);
        timer.schedule(interruptTask, timeoutMillis);

        // Run the service-specific startup / shutdown sequence

        var result = sequence.run(timeout);

        timer.cancel();

        // Check to see whether the sequence was interrupted

        if (thread.isInterrupted()) {
            throw new InterruptedException();
        }

        return result;
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
}
