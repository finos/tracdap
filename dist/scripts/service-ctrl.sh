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


APPLICATION_NAME="$applicationName"
APPLICATION_CLASS="$mainClassName"


# These environment variables are required to start the service

CONFIG_FILE="\${CONFIG_FILE}"


# Optional environment variables, set if needed, defaults are normally fine

ENABLE_PLUGINS=\${ENABLE_PLUGINS:=true}
ENABLE_PLUGINS_EXT=\${ENABLE_PLUGINS_EXT:=false}


# Control script settings

STARTUP_WAIT_TIME=3
SHUTDOWN_WAIT_TIME=30

# ----------------------------------------------------------------------------------------------------------------------


APP_HOME=\$(cd `dirname \$0` && cd .. && pwd)

CONFIG_DIR="\${APP_HOME}/config"
PLUGIN_DIR="\${APP_HOME}/plugins"
PLUGIN_EXT_DIR="\${APP_HOME}/plugins_ext"
LOG_DIR="\${APP_HOME}/log"
RUN_DIR="\${APP_HOME}/run"
PID_DIR="\${RUN_DIR}"

ENV_FILE="\${CONFIG_DIR}/env.sh"
PID_FILE="\${RUN_DIR}/${applicationName}.pid"

if [ -f "\${ENV_FILE}" ]; then
    . "\${ENV_FILE}"
fi

if [ ! -w \${PID_DIR} ]; then
    echo "PID directory is not writable: \${PID_DIR}"
    exit -1
fi

CORE_CLASSPATH=\$(cat <<-CLASSPATH_END
${classpath.replace(":", ":\\\n")}
CLASSPATH_END)

CORE_JAVA_OPTS=\$(cat <<-JAVA_OPTS_END
${defaultJvmOpts.replace("'", "").replace(' "-', '\n"-').replace('"', '')}
JAVA_OPTS_END)


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

    echo "Install location: \${APP_HOME}"
    echo "Config: \${CONFIG_FILE}"

    export CLASSPATH=\$CORE_CLASSPATH

    java \${CORE_JAVA_OPTS} \$APPLICATION_CLASS --config "\${CONFIG_FILE}" &
    PID=\$!

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
       echo "Usage: \$0 {start|stop|restart|status|kill|kill_all}"
esac

exit 0
