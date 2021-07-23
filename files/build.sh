#!/bin/bash

base_dir=$(dirname $0)

rm -fr ${base_dir}/clickhouse_data/
chmod +x ${base_dir}/clickhouse
cat pipe_file| ${base_dir}/clickhouse local \
--queries-file=build.sql \
--config-file=${base_dir}/config.xml \
--verbose --logger.level=trace --stacktrace