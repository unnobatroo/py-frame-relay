FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd -r relay && mkdir /data && chown relay /data

USER relay
VOLUME /data
EXPOSE 9000
# Exec form so python is PID 1 and receives SIGTERM from `docker stop`.
CMD ["python", "-m", "py_frame_relay", "--host", "0.0.0.0", "--log", "/data/telemetry.log"]
