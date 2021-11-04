# this is a Powershell Script to test PHP 7.1-7.4 and PHP 8.0 compatibility

# docker run --rm $(docker build --build-arg PHP_VERSION=7.1 -q -f Dockerfile.template .)
# docker run --rm $(docker build --build-arg PHP_VERSION=7.2 -q -f Dockerfile.template .)
docker run --rm $(docker build --build-arg PHP_VERSION=7.3.32 -q -f Dockerfile.template .)
docker run --rm $(docker build --build-arg PHP_VERSION=7.4 -q -f Dockerfile.template .)
docker run --rm $(docker build --build-arg PHP_VERSION=8.0 -q -f Dockerfile.template .)
