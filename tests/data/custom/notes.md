# Using Windows Powershell/Terminal
docker run --rm -v "$(pwd):/workdir" -w /workdir ykursadkaya/pyspark generate.py
docker run --rm -v "$(pwd):/workdir" -w /workdir ykursadkaya/pyspark convertToJson.py

# Interactive
docker run --rm -it --entrypoint /bin/bash -v "$(pwd):/workdir" -w /workdir ykursadkaya/pyspark
