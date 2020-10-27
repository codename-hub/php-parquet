FROM php:7.3-cli

# get apt-get lists for the first time
RUN apt-get update

## install zip extension using debian buster repo (which is now available)
## we need zip-1.14 or higher and libzip 1.2 or higher for ZIP encryption support
RUN apt-get update && apt-get install -t buster -y zlib1g-dev libzip-dev \
    && pecl install zip \
    && docker-php-ext-configure zip --with-libzip \
    && docker-php-ext-install zip

RUN apt-get install -y libgmp-dev

# install some php extensions
RUN docker-php-ext-install gmp bcmath

RUN apt-get install -y git

RUN git clone --recursive --depth=1 https://github.com/kjdev/php-ext-snappy.git \
  && cd php-ext-snappy \
  && phpize \
  && ./configure \
  && make \
  && make install \
  && docker-php-ext-enable snappy \
  && ls -lna

RUN yes | pecl install xdebug \
  # disabled by default
  # && docker-php-ext-enable xdebug \
  # && echo "zend_extension=$(find /usr/local/lib/php/extensions/ -name xdebug.so)" > /usr/local/etc/php/conf.d/xdebug.ini \
  && echo "xdebug.remote_enable=0" >> /usr/local/etc/php/conf.d/xdebug.ini \
  && echo "xdebug.profiler_enable_trigger=0" >> /usr/local/etc/php/conf.d/xdebug.ini \
  && echo "xdebug.profiler_output_dir=/tmp/profiler" >> /usr/local/etc/php/conf.d/xdebug.ini \
  && echo "xdebug.remote_host=host.docker.internal" >> /usr/local/etc/php/conf.d/xdebug.ini \
  && echo "xdebug.remote_connect_back=0" >> /usr/local/etc/php/conf.d/xdebug.ini \
  && echo "xdebug.remote_port=9001" >> /usr/local/etc/php/conf.d/xdebug.ini \
  && echo "xdebug.remote_log=/tmp/php-xdebug.log" >> /usr/local/etc/php/conf.d/xdebug.ini \
  && echo "xdebug.remote_autostart=0" >> /usr/local/etc/php/conf.d/xdebug.ini
