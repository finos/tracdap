@echo off >nul

REM Copyright 2020 Accenture Global Solutions Limited
REM
REM Licensed under the Apache License, Version 2.0 (the "License");
REM you may not use this file except in compliance with the License.
REM You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

echo Updating settings for IntelliJ IDEA...

set ide_source_dir=%~dp0%idea
set ide_target_dir=%~dp0%..\..\.idea

REM Normalize target path
for %%i in ("%ide_target_dir%") do SET "ide_target_dir=%%~fi"

call :copyRecursive %ide_source_dir% %ide_target_dir%
goto :eof

:copyRecursive

    REM make sure target dir exists
    call :mkdirPermissive %2

    REM copy files in the source directory
    for %%f in (%1\*) do (
        echo Updating -^> %2\%%~nxf
        copy /y "%%f" "%2" >nul
    )

    REM recurse sub directories
    for /D %%d in (%1\*) do (call :copyRecursive %%1\%%~nd %%2\%%~nd)

exit /b

:mkdirPermissive
    if not exist %1 (mkdir %1)
exit /b
