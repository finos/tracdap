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
setlocal EnableDelayedExpansion

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

@rem -------------------------------------------------------------------------------------------------------------------


@rem Variables in this section are set up automatically and should not be overridden

set SCRIPT_CMD=%0
set APPLICATION_NAME=$applicationName
set APPLICATION_CLASS=$mainClassName

set PID_FILE=%PID_DIR%${applicationName}.pid


@rem If CONFIG_FILE is relative, look in the config folder
if not "%CONFIG_FILE%" == "" (
if not "%CONFIG_FILE:~0,1%" == "\\" (
if not "%CONFIG_FILE:~0,1%" == "/" (
if not "%CONFIG_FILE:~1,1%" == ":" (

    if "%CONFIG_FILE:~0,4%" == "etc\\" (
        set CONFIG_FILE=%APP_HOME%%CONFIG_FILE%
    ) else if "%CONFIG_FILE:~0,4%" == "etc/" (
        set CONFIG_FILE=%APP_HOME%%CONFIG_FILE%
    ) else (
        set CONFIG_FILE=%CONFIG_DIR%%CONFIG_FILE%
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
    exit /b 1
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


goto :main


:run

    echo Running application: %APPLICATION_NAME%

    if "%CONFIG_FILE%" == "" (
        echo Missing required environment variable CONFIG_FILE
        exit /b 1
    )

    if exist "%PID_FILE%" (
        echo Application is already running, try %SCRIPT_CMD% [stop^|kill]
        exit /b 1
    )

    echo Java location: [%JAVA_CMD%]
    echo Install location: ^[%APP_HOME%]
    echo Working directory: [%RUN_DIR%]
    echo Config file: [%CONFIG_FILE%]
    echo.

    set CWD=%cd%
    cd "%RUN_DIR%"
    if "%SECRET_KEY%" == "" (
        "%JAVA_CMD%" %JAVA_OPTS% %APPLICATION_CLASS% --config "%CONFIG_FILE%" %*
    ) else (
        "%JAVA_CMD%" %JAVA_OPTS% %APPLICATION_CLASS% --config "%CONFIG_FILE%" --secret-key "%SECRET_KEY%" %*
    )
    set RESULT=%errorlevel%
    cd "%CWD%"

exit /b %RESULT%


:start

    echo Starting application: %APPLICATION_NAME%

    if "%CONFIG_FILE%" == "" (
        echo Missing required environment variable CONFIG_FILE
        exit /b 1
    )

    if exist "%PID_FILE%" (
        echo Application is already running, try %SCRIPT_CMD% [stop^|kill]
        exit /b 1
    )

    echo Java location: [%JAVA_CMD%]
    echo Install location: ^[%APP_HOME%]
    echo Working directory: [%RUN_DIR%]
    echo Config file: [%CONFIG_FILE%]
    echo.

    if "%SECRET_KEY%" == "" (
        start "%APPLICATION_NAME%" /D "%RUN_DIR%" /B "%JAVA_CMD%" %JAVA_OPTS% %APPLICATION_CLASS% --config "%CONFIG_FILE%" %*
    ) else (
        start "%APPLICATION_NAME%" /D "%RUN_DIR%" /B "%JAVA_CMD%" %JAVA_OPTS% %APPLICATION_CLASS% --config "%CONFIG_FILE%" --secret-key "%SECRET_KEY%" %*
    )

    @rem Look up PID using WMIC, name='java.exe' stops wmic from finding itself
    @rem findstr filters out blank lines, which are included in the output
    @rem sort orders by most recently started process, headers on top
    @rem skip headers (skip=1) and select second token (processId) as %%q

    for /f "usebackq skip=1 tokens=1,2" %%p in (
        `wmic process
            where "commandline like '%%%APPLICATION_CLASS%%%' and name='java.exe'"
            get creationDate^, processId 2^>nul ^|
        findstr /r /v "^\$" ^|
        sort /r`
    ) do set PID=%%q & goto got_pid
    :got_pid

    @rem Before recording the PID, wait to make sure the service doesn't crash on startup
    @rem Not a fail-safe guarantee, but will catch e.g. missing or invalid config files

    set COUNTDOWN=%STARTUP_WAIT_TIME%
    :start_countdown

        @rem Use wmic to check for the process explicitly by PID (always returns zero)
        @rem findstr returns an error code if there is no match

        wmic process where "processid=%PID%" get processid 2>nul | findstr "%PID%" >nul 2>nul

        if %errorlevel% equ 0 ( if !COUNTDOWN! gtr 0 (
            timeout 1 >nul
            set /a COUNTDOWN=!COUNTDOWN!-1
            goto start_countdown
        ))

    if %COUNTDOWN% equ 0 (
        echo %PID% > "%PID_FILE%"
    ) else (
        echo %APPLICATION_NAME% failed to start
        exit /b 1
    )

exit /b 0


:stop

    echo Stopping application: %APPLICATION_NAME%

    if not exist "%PID_FILE%" (

        echo %APPLICATION_NAME% is not running

    ) else (

        @rem Query WMIC for the PID stored in the PID file
        for /f "delims=" %%p in (%PID_FILE%) do set PID=%%p
        wmic process where "processid=!PID!" get processid 2>nul | findstr "!PID!" >nul 2>nul

        @rem Do not send the TERM signal if the application has already been stopped
        if !errorlevel! neq 0 (
            echo %APPLICATION_NAME% has already stopped
        ) else (

            @rem taskkill /pid !PID! only works if a window is allocated, and still does not trigger clean shutdown
            @rem wmic process terminate works regardless of allocating a window, but does not trigger clean shutdown
            @rem The only option to make this work correctly is to bundle a binary that can send process signals

            @rem For clean shutdown on Windows, run processes in the foreground using the "run" command

            wmic process where "processid=!PID!" call terminate >nul 2>nul

            @rem Wait for the application to go down cleanly
            set COUNTDOWN=%SHUTDOWN_WAIT_TIME%
            :stop_countdown
                wmic process where "processid=%PID%" get processid 2>nul | findstr "%PID%" >nul 2>nul
                if !errorlevel! equ 0 ( if !COUNTDOWN! gtr 0 (
                    timeout 1 >nul
                    set /a COUNTDOWN=!COUNTDOWN!-1
                    goto stop_countdown
                ))

            @rem If the timeout expired, send a kill signal
            if !COUNTDOWN! equ 0 (
                echo %APPLICATION_NAME% did not stop in time, sending kill signal...
                taskkill /pid !PID! /f
                echo %APPLICATION_NAME% has been killed
            )
        )

        del "%PID_FILE%"
    )

exit /b 0


:status

    if not exist "%PID_FILE%" (

        echo %APPLICATION_NAME% is down

    ) else (

        @rem Query WMIC for the PID stored in the PID file
        for /f "delims=" %%p in (%PID_FILE%) do set PID=%%p
        wmic process where "processid=!PID!" get processid 2>nul | findstr "!PID!" >nul 2>nul

        if !errorlevel! equ 0 (
            echo %APPLICATION_NAME% is up
        ) else (
            echo %APPLICATION_NAME% is down
            @rem Clean up the PID file if it refers to a dead process
            del "%PID_FILE%"
        )
    )

exit /b 0


:kill_pid

    echo Killing application: %APPLICATION_NAME%

    if not exist "%PID_FILE%" (

        echo %APPLICATION_NAME% is not running

    ) else (

        @rem Query WMIC for the PID stored in the PID file
        for /f "delims=" %%p in (%PID_FILE%) do set PID=%%p
        wmic process where "processid=!PID!" get processid 2>nul | findstr "!PID!" >nul 2>nul

        @rem Do not send the TERM signal if the application has already been stopped
        if !errorlevel! neq 0 (
            echo %APPLICATION_NAME% has already stopped
        ) else (

            taskkill /pid !PID! /f
            echo %APPLICATION_NAME% has been killed
        )

        del "%PID_FILE%"
    )

exit /b 0


:kill_all

    set FOUND=0

    for /f "usebackq skip=1" %%p in (
        `wmic process
            where "commandline like '%%%APPLICATION_CLASS%%%' and name='java.exe'"
            get processId 2^>nul ^|
        findstr /r /v "^\$" 2^>nul`
    ) do (
        set PID=%%p
        echo Killing PID !PID!...
        taskkill /pid !PID! /f
        set FOUND=1
    )

    if %FOUND% equ 0 (
        echo No processes found for %APPLICATION_NAME%
    )

    if exist "%PID_FILE%" (
        del "%PID_FILE%"
    )

exit /b 0


:main

    set CMD=%1
    set ARGS=
    shift

    :arg_loop
        if "%1"=="" goto arg_loop_done
        set ARGS=%ARGS% %1
        shift
    goto arg_loop
    :arg_loop_done

    if not "%CMD%" == "run" (
        @rem If the PID directory is not writable, don't even try to start
        echo. > "%PID_DIR%${applicationName}.test"
        del "%PID_DIR%${applicationName}.test"
        if errorlevel 1 (
            echo "PID directory is not writable: %PID_DIR%"
            exit /b 1
        )
    ) else (
        if not exist "%PID_DIR%" (
            echo "PID directory does not exist: %PID_DIR%"
            exit /b 1
        )
    )

    if "%CMD%" == "run" (
        echo %ARGS%
        call :run %ARGS%
    ) else if "%CMD%" == "start" (
        call :start %ARGS%
    ) else if "%CMD%" == "stop" (
         call :stop
    ) else if "%CMD%" == "restart" (
        call :stop && call :start
    ) else if "%CMD%" == "status" (
        call :status
    ) else if "%CMD%" == "kill" (
        call :kill_pid
    ) else if "%CMD%" == "kill_all" (
        call :kill_all
    ) else (
        echo Usage: %SCRIPT_CMD% ^[run^|start^|stop^|restart^|status^|kill^|kill_all^]
    )

exit /b %errorlevel%
