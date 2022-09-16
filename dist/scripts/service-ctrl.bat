@rem Copyright 2022 Accenture Global Solutions Limited
@rem
@rem Licensed under the Apache License, Version 2.0 (the "License");
@rem you may not use this file except in compliance with the License.
@rem You may obtain a copy of the License at
@rem
@rem      https://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.

@rem Control script for ${applicationName}

@rem The following environment variables are used to start the application:
@rem
@rem   CONFIG_FILE - defaults to %APP_HOME%\\etc\\<DEFAULT_CONFIG_FILE>
@rem
@rem   PLUGINS_ENABLED - load plugins from %APP_HOME%\\plugins, defaults to true
@rem   PLUGINS_EXT_ENABLED - load plugins from %APP_HOME%\\plugins_ext, defaults to false
@rem
@rem You may also wish to set these variables to control the JVM:
@rem
@rem   JAVA_HOME
@rem   JAVA_OPTS
@rem
@rem All these can be set directly before calling this script, or via the env.sh
@rem in the etc directory.

@rem -------------------------------------------------------------------------------------------------------------------

@echo off
setlocal

@rem Variables in this section do not need to be set, but can be overridden if needed in env.bat.

@rem Find the installation folder
for %%A in ("%~dp0.") do set APP_HOME=%%~dpA


@rem Set up the default folder structure (this can be overridden in env.sh if required)
set CONFIG_DIR=%APP_HOME%etc\\
set PLUGINS_DIR=%APP_HOME%plugins\\
set PLUGINS_EXT_DIR=%APP_HOME%plugins_ext\\
set LOG_DIR=%APP_HOME%log\\
set RUN_DIR=%APP_HOME%run\\
set PID_DIR=%RUN_DIR%

if "%CONFIG_FILE%" == "" (set CONFIG_FILE=%APP_HOME%etc\\<DEFAULT_CONFIG_FILE>)
set ENV_FILE=%CONFIG_DIR%env.bat

if "%PLUGINS_ENABLED%" == "" (set PLUGINS_ENABLED=true)
if "%PLUGINS_EXT_ENABLED%" == "" (set PLUGINS_EXT_ENABLED=false)

set STARTUP_WAIT_TIME=3
set SHUTDOWN_WAIT_TIME=30

@rem Any variables set before this point can be overridden by the env file
if exist "%ENV_FILE%" (
    call "%ENV_FILE%"
)

@rem If the PID directory is not writable, don't even try to start
echo. > "%PID_DIR%${applicationName}.test"
del "%PID_DIR%${applicationName}.test"
if errorlevel 1 (
    echo "PID directory is not writable: %PID_DIR%"
    exit /b -1
)

@rem -------------------------------------------------------------------------------------------------------------------


@rem Variables in this section are set up automatically and should not be overridden

set APPLICATION_NAME=$applicationName
set APPLICATION_CLASS=$mainClassName

set PID_FILE=%PID_DIR%${applicationName}.pid

@rem If CONFIG_FILE is relative, look in the config folder
if not "%CONFIG_FILE%" == "" (
if not "%CONFIG_FILE:~0,1%" == "\\" (
if not "%CONFIG_FILE:~0,1%" == "/" (
if not "%CONFIG_FILE:~1,1%" == ":" (

    if "%CONFIG_FILE:~0,4%" == "etc\\" (
        set CONFIG_FILE="%APP_HOME%%CONFIG_FILE%"
    ) else if "%CONFIG_FILE:~0,4%" == "etc/" (
        set CONFIG_FILE="%APP_HOME%%CONFIG_FILE%"
    ) else (
        set CONFIG_FILE="%CONFIG_DIR%%CONFIG_FILE%"
    )

))))


@rem Discover Java

if not "%JAVA_HOME%" == "" (
    set JAVA_CMD=%JAVA_HOME%\\bin\\java.exe
) else (
    set JAVA_CMD=java.exe
)

"%JAVA_CMD%" -version >NUL 2>&1
if errorlevel 1 (
    if not "%JAVA_HOME%" == "" (
        echo JAVA_HOME does not contain a valid Java installation: [%JAVA_HOME%]
    ) else (
        echo JAVA_HOME is not set and no 'java.exe' command could be found in PATH
    )
    exit /b -1
)


@rem Core classpath is supplied by the build system and should never be edited directly

set CLASSPATH=${classpath.replace(";", "\nset CLASSPATH=%CLASSPATH%;").replace("%APP_HOME%\\", "%APP_HOME%")}

@rem Discover standard plugins

if "%PLUGINS_ENABLED%" == "true" (
    for %%j in (%PLUGINS_DIR%*.jar) do (
        set CLASSPATH=%CLASSPATH%;%%j
    )
)

@rem Discover external plugins

if "%PLUGINS_EXT_ENABLED%" == "true" (
    for %%j in (%PLUGINS_EXT_DIR%*.jar) do (
        set CLASSPATH=%CLASSPATH%;%%j
    )
)


@rem Core Java opts are supplied by the build system and should never be edited directly

set CORE_JAVA_OPTS=${defaultJvmOpts.replace(' "-', '\nset CORE_JAVA_OPTS=%CORE_JAVA_OPTS% "-').replace('"', '')}

@rem Set java properties used for logging

set CORE_JAVA_OPTS=%CORE_JAVA_OPTS% -DLOG_DIR=%LOG_DIR%
set CORE_JAVA_OPTS=%CORE_JAVA_OPTS% -DLOG_NAME=%APPLICATION_NAME%

@rem Add core Java opts to opts supplied from the environment or env.sh

set JAVA_OPTS=%CORE_JAVA_OPTS% %JAVA_OPTS%

@rem -------------------------------------------------------------------------------------------------------------------


echo CONFIG_DIR = %CONFIG_DIR%
echo PLUGINS_DIR = %PLUGINS_DIR%
echo PLUGINS_EXT_DIR = %PLUGINS_EXT_DIR%
echo LOG_DIR = %LOG_DIR%
echo RUN_DIR = %RUN_DIR%
echo PID_DIR = %PID_DIR%

echo CONFIG_FILE = %CONFIG_FILE%
echo ENV_FILE = %ENV_FILE%
echo PLUGINS_ENABLED = %PLUGINS_ENABLED%
echo PLUGINS_EXT_ENABLED = %PLUGINS_EXT_ENABLED%
echo STARTUP_WAIT_TIME = %STARTUP_WAIT_TIME%
echo SHUTDOWN_WAIT_TIME = %SHUTDOWN_WAIT_TIME%

echo APPLICATION_NAME = %APPLICATION_NAME%
echo APPLICATION_CLASS = %APPLICATION_CLASS%
echo PID_FILE = %PID_FILE%

echo JAVA_HOME = %JAVA_HOME%
echo JAVA_CMD = %JAVA_CMD%

@rem echo CLASSPATH = %CLASSPATH%

echo CORE_JAVA_OPTS = %CORE_JAVA_OPTS%
echo JAVA_OPTS = %JAVA_OPTS%
