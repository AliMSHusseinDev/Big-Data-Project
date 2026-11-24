FROM python:3.11.13-slim-bookworm
WORKDIR /app
COPY requirements.txt /build/requirements.txt
# A cloud builder may supply its trusted CA bundle as a BuildKit secret.
RUN --mount=type=secret,id=build_ca,mode=0444 \
    set -eu; ca=/etc/ssl/certs/ca-certificates.crt; \
    if [ -f /run/secrets/build_ca ]; then ca=/run/secrets/build_ca; fi; \
    sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources; \
    apt-get -o Acquire::https::CaInfo="$ca" update; \
    apt-get -o Acquire::https::CaInfo="$ca" install -y --no-install-recommends curl unixodbc odbcinst; \
    curl --cacert "$ca" -fsSL https://packages.microsoft.com/debian/12/prod/pool/main/m/msodbcsql18/msodbcsql18_18.5.1.1-1_amd64.deb -o /tmp/driver.deb; \
    echo 'f484d54869110529eb84ddb9261ed5a0aa6258ba15027b83da931a9821ed3664  /tmp/driver.deb' | sha256sum -c -; \
    ACCEPT_EULA=Y dpkg -i /tmp/driver.deb; \
    pip install --cert "$ca" --no-cache-dir -r /build/requirements.txt; \
    rm -rf /var/lib/apt/lists/* /tmp/driver.deb
ENV PYTHONPATH=/app PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
