FROM php:7.3.32-cli

# get apt-get lists for the first time
RUN apt-get update

## install zip extension using debian buster repo (which is now available)
## we need zip-1.14 or higher and libzip 1.2 or higher for ZIP encryption support
RUN apt-get update && apt-get install -y zlib1g-dev libzip-dev \
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

# install PCOV for faster generation of coverage reports instead of xdebug
RUN  pecl install pcov-1.0.9 \
  && docker-php-ext-enable pcov
