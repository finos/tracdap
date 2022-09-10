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

CONFIG_FILE="\${CONFIG_FILE:?Missing required environment variable CONFIG_FILE}"


# Optional environment variables, set if needed, defaults are normally fine

ENABLE_PLUGINS=\${ENABLE_PLUGINS:=true}
ENABLE_PLUGINS_EXT=\${ENABLE_PLUGINS_EXT:=false}


# Standard directory locations, relative to the install dir

PLUGIN_DIR="plugins"
PLUGIN_EXT_DIR="plugins_ext"
LOG_DIR="log"
RUN_DIR="run"


# JVM settings

DEFAULT_JVM_OPTS=$defaultJvmOpts


# ----------------------------------------------------------------------------------------------------------------------

APP_HOME=\$(cd `dirname \$0` && cd .. && pwd)

CORE_CLASSPATH=\$(cat <<-CLASSPATH_END
${classpath.replace(":", ":\\\n")}
CLASSPATH_END)


start() {

    echo "Starting application: \${APPLICATION_NAME}"
    echo

    echo "Application install location: \${APP_HOME}"
    echo "Application config: \${CONFIG_FILE}"
    echo

    java -cp \$CORE_CLASSPATH \$APPLICATION_CLASS --config "\${CONFIG_FILE}"
}

stop() {
    echo "This will stop \${APPLICATION_CLASS}"
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
       # code to check status of app comes here
       # example: status program_name
       ;;
    *)
       echo "Usage: \$0
       } {start|stop|status|restart}"
esac

exit 0
