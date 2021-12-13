
FROM python:3.10-slim as builder

COPY build/dist /tmp/dist

RUN mkdir -p /opt/trac/runtime-python
RUN python -m venv /opt/trac/runtime-python/venv
RUN . /opt/trac/runtime-python/venv/bin/activate && pip install `ls /tmp/dist/trac_runtime-*.whl`


FROM python:3.10-slim
COPY --from=builder /opt/trac /opt/trac
