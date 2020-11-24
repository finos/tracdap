@echo off >nul
set start_dir=%cd%

echo Updating settings for IntelliJ IDEA...

set ide_source_dir=%~dp0%idea
set ide_target_dir=%~dp0%..\..\.idea

call :copyRecursive %ide_source_dir% %ide_target_dir%
goto :done


:copyRecursive

    REM make sure target dir exists
    call :mkdirPermissive %2

    REM copy files in the source directory
    for %%f in (%1\*) do (
        echo Updating -^> %%f
        copy /y "%%f" "%2" >nul
    )

    REM recurse sub directories
    for /D %%d in (%1\*) do (call :copyRecursive %%1\%%~nd %%2\%%~nd)

exit /b


:mkdirPermissive
    if not exist %1 (mkdir %1)
exit /b


:done
cd /d %start_dir%
