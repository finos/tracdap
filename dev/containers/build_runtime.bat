
git fetch --tags
git remote | findstr /i "upstream" > nul && git fetch upstream --tags

docker run --mount type=bind,source=%~dp0..\..,target=/mnt/trac python:3.10 /mnt/trac/dev/containers/build_runtime_inner.sh
