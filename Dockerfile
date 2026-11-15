FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    COLLECTOR_DATA_DIR=/data \
    COLLECTOR_HOST=0.0.0.0 \
    COLLECTOR_PORT=8000

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py errors.py ingest.py manifest.py downloader.py store.py transport.py ./
COPY templates ./templates
COPY sample_source ./sample_source

RUN useradd --create-home --uid 10001 collector \
 && mkdir -p /data/files /data/partials \
 && chown -R collector:collector /data /srv
USER collector

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status==200 else 1)"

CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "4", \
     "--preload", "--timeout", "120", "--access-logfile", "-", "app:create_app()"]
