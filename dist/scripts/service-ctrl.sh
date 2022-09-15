#!/bin/sh

# Copyright 2022 Accenture Global Solutions Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Control script for ${applicationName}

# The following environment variables are used to start the application:
#
#   CONFIG_FILE - defaults to \${APP_HOME}/etc/<DEFAULT_CONFIG_FILE>
#
#   PLUGINS_ENABLED - load plugins from \${APP_HOME}/plugins, defaults to true
#   PLUGINS_EXT_ENABLED - load plugins from \${APP_HOME}/plugins_ext, defaults to false
#
# You may also wish to set these variables to control the JVM:
#
#   JAVA_HOME
#   JAVA_OPTS
#
# All these can be set directly before calling this script, or via the env.sh
# in the etc directory.

# ----------------------------------------------------------------------------------------------------------------------


# Variables in this section do not need to be set, but can be overridden if needed in env.sh.

# Find the installation folder
APP_HOME=\$(cd `dirname \$0` && cd .. && pwd)

# Set up the default folder structure (this can be overridden in env.sh if required)
CONFIG_DIR="\${APP_HOME}/etc"
PLUGINS_DIR="\${APP_HOME}/plugins"
PLUGINS_EXT_DIR="\${APP_HOME}/plugins_ext"
LOG_DIR="\${APP_HOME}/log"
RUN_DIR="\${APP_HOME}/run"
PID_DIR="\${RUN_DIR}"

CONFIG_FILE="\${CONFIG_FILE:=\${APP_HOME}/etc/<DEFAULT_CONFIG_FILE>}"
ENV_FILE="\${CONFIG_DIR}/env.sh"

PLUGINS_ENABLED="\${PLUGINS_ENABLED:=true}"
PLUGINS_EXT_ENABLED="\${PLUGINS_EXT_ENABLED:=false}"

STARTUP_WAIT_TIME=3
SHUTDOWN_WAIT_TIME=30

# Any variables set before this point can be overridden by the env file
if [ -f "\${ENV_FILE}" ]; then
    . "\${ENV_FILE}"
fi

# If the PID directory is not writable, don't even try to start
if [ ! -w "\${PID_DIR}" ]; then
    echo "PID directory is not writable: \${PID_DIR}"
    exit -1
fi

# ----------------------------------------------------------------------------------------------------------------------


# Variables in this section are set up automatically and should not be overridden

APPLICATION_NAME="$applicationName"
APPLICATION_CLASS="$mainClassName"

PID_FILE="\${PID_DIR}/${applicationName}.pid"

# If CONFIG_FILE is relative, look in the config folder
if [ "\${CONFIG_FILE}" != "" ] && [ "\${CONFIG_FILE:0:1}" != "/" ]; then
    CONFIG_FILE="\${CONFIG_DIR}/\${CONFIG_FILE}"
fi


# Discover Java

if [ -n "\$JAVA_HOME" ] ; then

    if [ -x "\$JAVA_HOME/jre/sh/java" ] ; then
        # IBM's JDK on AIX uses strange locations for the executables
        JAVA_CMD=\$JAVA_HOME/jre/sh/java
    else
        JAVA_CMD=\$JAVA_HOME/bin/java
    fi

    if [ ! -x "\$JAVA_CMD" ] ; then
        echo "JAVA_HOME does not contain a valid Java installation: [\${JAVA_HOME}]"
        exit -1
    fi

else

    JAVA_CMD=`which java`

    if [ ! -x "\$JAVA_CMD" ] ; then
        echo "JAVA_HOME is not set and no 'java' command could be found in PATH"
        exit -1
    fi

fi


# Core classpath is supplied by the build system and should never be edited directly

CLASSPATH=\$(cat <<-CLASSPATH_END
${classpath.replace(":", ":\\\n")}
CLASSPATH_END)

# Discover standard plugins

if [ "\${PLUGINS_ENABLED}" = "true" ]; then
    CWD=`pwd` && cd "\${PLUGINS_DIR}"
    for JAR in `find . -name "*.jar" | sed s#^./##`; do
        CLASSPATH="\${CLASSPATH}:\${PLUGINS_EXT_DIR}/\${JAR}";
    done
    cd "\${CWD}"
fi

# Discover external plugins

if [ "\${PLUGINS_EXT_ENABLED}" = "true" ]; then
    CWD=`pwd` && cd "\${PLUGINS_EXT_DIR}"
    for JAR in `find . -name "*.jar" | sed s#^./##`; do
        CLASSPATH="\${CLASSPATH}:\${PLUGINS_EXT_DIR}/\${JAR}";
    done
    cd "\${CWD}"
fi


# Core Java opts are supplied by the build system and should never be edited directly

CORE_JAVA_OPTS=\$(cat <<-JAVA_OPTS_END
${defaultJvmOpts.replace("'", "").replace(' "-', '\n"-').replace('"', '')}
JAVA_OPTS_END)

# Add core Java opts to opts supplied from the environment or env.sh

JAVA_OPTS="\${CORE_JAVA_OPTS} \${JAVA_OPTS}"

# ----------------------------------------------------------------------------------------------------------------------


run() {

    echo "Running application: \${APPLICATION_NAME}"

    if [ "\${CONFIG_FILE}" == "" ]; then
        echo "Missing required environment variable CONFIG_FILE"
        exit -1
    fi

    if [ -f \${PID_FILE} ]; then
      echo "Application is already running, try \$0 [stop | kill]"
      exit -1
    fi

    echo "Java location: [\${JAVA_CMD}]"
    echo "Install location: [\${APP_HOME}]"
    echo "Working directory: [\${RUN_DIR}]"
    echo "Config file: [\${CONFIG_FILE}]"
    echo

    if [ \$# -gt 0 ]; then
        TASK_LIST=""
        for TASK in \$@; do
            echo "Task: \$TASK"
            TASK_LIST="\${TASK_LIST} --task \"\${TASK}\""
        done
        echo
    fi

    export CLASSPATH

    CWD=`pwd`
    cd "\${RUN_DIR}"
    java \${JAVA_OPTS} \$APPLICATION_CLASS --config "\${CONFIG_FILE}" &
    cd "\${CWD}"
}

start() {

    echo "Starting application: \${APPLICATION_NAME}"

    if [ "\${CONFIG_FILE}" == "" ]; then
        echo "Missing required environment variable CONFIG_FILE"
        exit -1
    fi

    if [ -f \${PID_FILE} ]; then
      echo "Application is already running, try \$0 [stop | kill]"
      exit -1
    fi

    echo "Java location: [\${JAVA_CMD}]"
    echo "Install location: [\${APP_HOME}]"
    echo "Working directory: [\${RUN_DIR}]"
    echo "Config file: [\${CONFIG_FILE}]"
    echo

    export CLASSPATH

    CWD=`pwd`
    cd "\${RUN_DIR}"
    java \${JAVA_OPTS} \$APPLICATION_CLASS --config "\${CONFIG_FILE}" &
    PID=\$!
    cd "\${CWD}"

    # Before recording the PID, wait to make sure the service doesn't crash on startup
    # Not a fail-safe guarantee, but will catch e.g. missing or invalid config files

    COUNTDOWN=\$STARTUP_WAIT_TIME
    while `ps -p \$PID > /dev/null` && [ \$COUNTDOWN -gt 0 ]; do
        sleep 1
        COUNTDOWN=\$((COUNTDOWN - 1))
    done

    if [ \$COUNTDOWN -eq 0 ]; then
        echo \$PID > "\${PID_FILE}"
    else
        echo "\${APPLICATION_NAME} failed to start"
        exit -1
    fi
}

stop() {

    echo "Stopping application: \${APPLICATION_NAME}"

    if [ ! -f "\${PID_FILE}" ]; then

        echo "\${APPLICATION_NAME} is not running"

    else

        PID=`cat "\${PID_FILE}"`

        # Do not send the TERM signal if the application has already been stopped
        if `ps -p \$PID > /dev/null`; then

            kill -TERM \$PID

            # Wait for the application to go down cleanly
            COUNTDOWN=\$SHUTDOWN_WAIT_TIME
            while `ps -p \$PID > /dev/null` && [ \$COUNTDOWN -gt 0 ]; do
                sleep 1
                COUNTDOWN=\$((COUNTDOWN - 1))
            done

            # If the timeout expired, send a kill signal
            if [ \$COUNTDOWN -eq 0 ]; then
                echo "\${APPLICATION_NAME} did not stop in time"
                kill -KILL \$PID
                echo "\${APPLICATION_NAME} has been killed"
            fi

        else

            echo "\${APPLICATION_NAME} has already stopped"

        fi

        rm "\${PID_FILE}"
    fi
}

status() {

    if [ ! -f "\${PID_FILE}" ]; then

        echo "\${APPLICATION_NAME} is down"

    else

        PID=`cat "\${PID_FILE}"`

        if [ `ps -p \$PID > /dev/null` ]; then

            echo "\${APPLICATION_NAME} is up"

        else

            echo "\${APPLICATION_NAME} is down"

            # Clean up the PID file if it refers to a dead process
            rm "\${PID_FILE}"
        fi
    fi
}

kill_pid() {

    echo "Killing application: \${APPLICATION_NAME}"

    if [ ! -f "\${PID_FILE}" ]; then

        echo "\${APPLICATION_NAME} is not running"

    else

        PID=`cat "\${PID_FILE}"`

        # Do not send the TERM signal if the application has already been stopped
        if `ps -p \$PID > /dev/null`; then
            kill -TERM \$PID
            echo "\${APPLICATION_NAME} has been killed"
        else
            echo "\${APPLICATION_NAME} has already stopped"
        fi

        rm "\${PID_FILE}"
    fi
}

kill_all() {

    FOUND=0

    for PID in `ps -A | grep "[j]ava" | grep "\${APPLICATION_CLASS}" | awk '{print \$1;}'`; do
        echo "Killing PID \$PID..."
        kill -KILL \$PID
        FOUND=1
    done

    if [ \$FOUND -eq 0 ]; then
        echo "No processes found for \${APPLICATION_NAME}"
    fi

    if [ -f "\${PID_FILE}" ]; then
        rm "\${PID_FILE}"
    fi
}


case "\$1" in
    run)
       shift
       run \$@
       ;;
    start)
       start
       ;;
    stop)
       stop
       ;;
    restart)
       stop
       start
       ;;
    status)
       status
       ;;
    kill)
       kill_pid
       ;;
    kill_all)
       kill_all
       ;;
    *)
       echo "Usage: \$0 {run|start|stop|restart|status|kill|kill_all}"
esac

exit 0
