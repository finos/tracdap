@echo off

set BUILD_IMAGE=python:3.10
set TRAC_IMAGE="trac/runtime-python"

REM Get repo root dir
for %%A IN ("%~dp0..") do set REPO_DIR=%%~dpA


REM Look up TRAC version
echo Looking up TRAC version...

git fetch --tags
git remote | findstr /i "upstream" > nul && git fetch upstream --tags

for /f "tokens=* USEBACKQ" %%v in (
    `powershell -ExecutionPolicy Bypass -File %REPO_DIR%dev\version.ps1`) do (
    set TRAC_VERSION=%%v)

REM Docker cannot handle "+dev" version suffixes
REM For anything that is not a tagged release, use version DEVELOPMENT for the image tag

if not %TRAC_VERSION:+dev=%==%TRAC_VERSION% (
    set TRAC_VERSION=DEVELOPMENT)

echo TRAC version = %TRAC_VERSION%


@echo on
docker run --mount type=bind,source=%REPO_DIR%,target=/mnt/trac %BUILD_IMAGE% /mnt/trac/dev/containers/build_runtime_inner.sh
docker build -t "%TRAC_IMAGE%:%TRAC_VERSION%" "%REPO_DIR%trac-runtime\python"
