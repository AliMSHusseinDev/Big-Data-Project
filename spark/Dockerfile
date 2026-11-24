FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu
USER root
COPY spark/download_jars.py /tmp/download_jars.py
RUN --mount=type=secret,id=build_ca python3 /tmp/download_jars.py && mkdir -p /checkpoints && chown 185:0 /checkpoints
USER 185
WORKDIR /app
ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1
