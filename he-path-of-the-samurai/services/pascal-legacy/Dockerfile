FROM debian:12-slim

# Установка компилятора Pascal и клиента Postgres
RUN apt-get update \
    && apt-get install -y --no-install-recommends fp-compiler postgresql-client ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/legacy

# Копируем исходники и скрипт запуска
COPY legacy.pas ./
COPY run.sh /usr/local/bin/run_legacy.sh
RUN chmod +x /usr/local/bin/run_legacy.sh

# Точка входа
CMD ["/usr/local/bin/run_legacy.sh"]
