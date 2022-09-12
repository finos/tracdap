@echo off

rem Copyright 2022 Accenture Global Solutions Limited
rem
rem Licensed under the Apache License, Version 2.0 (the "License");
rem you may not use this file except in compliance with the License.
rem You may obtain a copy of the License at
rem
rem      https://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.


set APPLICATION_NAME=$applicationName
set APPLICATION_CLASS=$mainClassName


rem These environment variables are required to start the service

if not defined CONFIG_FILE (
    echo Missing required environment variable CONFIG_FILE
    exit /B -1
)


rem Optional environment variables, set if needed, defaults are normally fine

if not defined ENABLE_PLUGINS (set ENABLE_PLUGINS=true)
if not defined ENABLE_PLUGINS_EXT (set ENABLE_PLUGINS_EXT=false)


rem --------------------------------------------------------------------------------------------------------------------

rem APP_HOME
for %%A in ("%~dp0.") do set APP_HOME=%%~dpA

set PLUGIN_DIR=%APP_HOME%\\plugins
set PLUGIN_EXT_DIR=%APP_HOME%\\plugins_ext

set PID_DIR=%APP_HOME%\\run
set PID_FILE=%PID_DIR%\\tracdap-svc-meta.pid

set CORE_CLASSPATH=""
set CORE_CLASSPATH=%CORE_CLASSPATH%;${classpath.replace(";", "\nset CORE_CLASSPATH=%CORE_CLASSPATH%;")}

set CORE_JAVA_OPTS=""
set CORE_JAVA_OPTS=%CORE_JAVA_OPTS% ${defaultJvmOpts.replace("'", "").replace(' \"-', '\nset CORE_JAVA_OPTS=%CORE_JAVA_OPTS% "-').replace('"', '')}




:start
    echo Starting application: %APPLICATION_NAME%
    echo.

    if not exist "%PID_DIR%" (
      echo PID dir does not exist: %PID_DIR%
      exit /B -1
    )

    if exist "%PID_FILE%" (
      echo Application is already running, try %%0 [stop | kill]
      exit /B -1
    )

    echo Application install location: %APP_HOME%
    echo Application config: %CONFIG_FILE%
    echo.

    set CLASSPATH=%CORE_CLASSPATH%

    set LAUNCH_CMD = java %CORE_JAVA_OPTS% %APPLICATION_CLASS% --config "%CONFIG_FILE%"

    for /f "tokens=2 delims==; " %%a in (
        'wmic process call create "%LAUNCH_CMD%" ^| find "ProcessId"'
    ) do set PID=%%a

    echo PID > "%PID_FILE%"

exit /B 0


:stop
    echo Stopping service: %APPLICATION_NAME%


exit /B 0


