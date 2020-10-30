# docker build -t php-parquet-test .
# docker run --rm -v "$(pwd):/home/php-parquet" -it --entrypoint bin/bash php-parquet-test
docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="vendor/bin/phpunit" php-parquet-test tests

# custom, right now
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="vendor/bin/phpunit" php-parquet-test tests/ListTest.php
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="vendor/bin/phpunit" php-parquet-test tests/SchemaTest.php
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="vendor/bin/phpunit" php-parquet-test tests/RepeatableFieldsTest.php
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="vendor/bin/phpunit" php-parquet-test tests/StructureTest.php
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="vendor/bin/phpunit" php-parquet-test tests/SnappyTest.php
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="vendor/bin/phpunit" php-parquet-test tests/CompressionTest.php
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="vendor/bin/phpunit" php-parquet-test tests/EndToEndTypeTest.php
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="vendor/bin/phpunit" php-parquet-test tests/values/primitives/BigDecimalTest.php

# interactive shell
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="bash" php-parquet-test

# profiling benchmark
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="php" php-parquet-test -d xdebug.profiler_enable=On -d xdebug.profiler_output_dir=. benchmark.php

# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="php" php-parquet-test -d zend_extension=/usr/local/lib/php/extensions/no-debug-non-zts-20180731/xdebug.so -d xdebug.profiler_enable=On -d xdebug.profiler_output_dir=. benchmark.php

# regular benchmark
# docker run --rm -v "$(pwd):/home/php-parquet" -it -w="/home/php-parquet" --entrypoint="php" php-parquet-test benchmark.php
