# NameNode image + Python 3, so Hadoop Streaming (mapreduce/*.py) runs without
# manual steps. Hadoop Streaming runs in local mode inside this container.
FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

# The base image is Debian 9 (Stretch); its packages moved to archive.debian.org.
RUN set -eux; \
    printf '%s\n' \
      'deb http://archive.debian.org/debian stretch main' \
      'deb http://archive.debian.org/debian-security stretch/updates main' \
      > /etc/apt/sources.list; \
    apt-get -o Acquire::Check-Valid-Until=false update; \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3; \
    rm -rf /var/lib/apt/lists/*; \
    python3 --version
# Python 3.5: mapreduce/*.py must avoid f-strings and other 3.6+ syntax.
# Entrypoint and NameNode start command are inherited unchanged.
