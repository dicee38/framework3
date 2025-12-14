FROM php:8.3-fpm-alpine

# Установка системных пакетов и расширений
RUN apk add --no-cache bash curl git unzip rsync postgresql-dev \
    && docker-php-ext-install pdo pdo_pgsql

# Composer
COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

# Создание директорий
RUN mkdir -p /var/www/html /opt/laravel-patches
WORKDIR /var/www/html

# Entrypoint
COPY entrypoint.sh /usr/local/bin/php_entrypoint.sh
RUN chmod +x /usr/local/bin/php_entrypoint.sh

CMD ["bash", "/usr/local/bin/php_entrypoint.sh"]
