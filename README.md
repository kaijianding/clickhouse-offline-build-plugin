# clickhouse-offline-build-plugin

### Why new clickhouse plugin?

The build-in clickhouse waterdrop plugin uses jdbc to insert data into clickhouse cluster.  
This can cause huge pressure to the clickhouse cluster and also data duplication due to retries.

### How this plugin work?

Basically, this plugin build clickhouse `part` locally with `clickhouse local` command, and upload the built `part` files to hdfs.  
Then the clickhouse node can pull the `part` from hdfs and call `alter table t attach part 'part_name'` to load data.

### How to use?
1. build this plugin by `mvn clean package` and get the clickhouse-offline-build-1.0.jar
2. build the plugins.tar.gz with structure
```
plugins/
plugins/clickhouse-offline-build/
plugins/clickhouse-offline-build/files/
plugins/clickhouse-offline-build/files/clickhouse
plugins/clickhouse-offline-build/files/config.xml
plugins/clickhouse-offline-build/files/build.sh
plugins/clickhouse-offline-build/lib/clickhouse-offline-build-1.0.jar
```
3. follow the steps on https://interestinglab.github.io/waterdrop-docs/#/zh-cn/v1/quick-start
4. with config
```
output {
  io.github.interestinglab.waterdrop.output.clickhouse.ClickhouseOfflineBuild {
    host = "${clickhouse_cluster}:8023"
    database = "${database}"
    table = "${tableName}"
    username = "${account}"
    password = "${password}"
    hdfsUser = "${hdfsUser}"
    tmpUploadPath = "${tmpUploadPath}"
    defaultValues = {"a":1} #optional
  }
}
```

### Local run example

0. clickhouse is ready, and `test.ontime` table is ready
1. follow https://interestinglab.github.io/waterdrop-docs/#/zh-cn/v1/installation to get waterdrop
2. prepare some `ontime` data: https://clickhouse.tech/docs/en/getting-started/example-datasets/ontime/
3. create config/application.conf
```shell
spark {
    spark.streaming.batchDuration = 5
    spark.app.name = "Waterdrop-sample"
    spark.ui.port = 13000
    spark.executor.instances = 2
    spark.executor.cores = 1
    spark.executor.memory = "1g"
}

input {
    file {
        path = "path_to_ontime_data.csv.gz"
        format = "csv"
        options.header = "true"
        result_table_name = "ontime"
    }
}

filter {
}

output {
    io.github.interestinglab.waterdrop.output.clickhouse.ClickhouseOfflineBuild {
        host = "127.0.0.1:8023"
        database = "test"
        table = "ontime"
        username = "default"
        password = "default_password"
        hdfsUser = "test_user"
        defaultValues = {"a":"b"}
        tmpUploadPath = "hdfs://remote_hdfs_host:9000/clickhouse-build/test/ontime/"
    }
}
```

4. run
```shell
./bin/start-waterdrop.sh --config config/application.conf --deploy-mode client --master "local[2]"
```

5. check data on `hdfs://remote_hdfs_host:9000/clickhouse-build/test/ontime/`
6. run script on clickhouse node: `sh attach.sh test ontime hdfs://remote_hdfs_host:9000/clickhouse-build/test/ontime/`
7. verify `select count(*) from test.ontime`