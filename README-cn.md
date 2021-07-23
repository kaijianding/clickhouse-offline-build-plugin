# clickhouse-offline-build-plugin

### 为什么需要新的clickhouse plugin?

waterdrop自带的clickhouse waterdrop使用jdbc去insert数据clickhouse集群.  
这可能对clickhouse集群造成很大压力, 并且因为重试的原因导致重复插入.

### 这个新插件是怎么运作的?

基本上, 这个插件使用clickhouse自带的`clickhouse local`命令在本地构建`part`文件, 然后把构建好的`part`文件上传到hdfs.  
然后再clickhouse节点从hdfs上把`part`文件拉下来放在detached目录, 再调用`alter table t attach part 'part_name'`加载数据

### 如何使用?
1. 先用maven打包`mvn clean package`得到clickhouse-offline-build-1.0.jar
2. 按如下目录结构准备plugins.tar.gz
```
plugins/
plugins/clickhouse-offline-build/
plugins/clickhouse-offline-build/files/
plugins/clickhouse-offline-build/files/clickhouse
plugins/clickhouse-offline-build/files/config.xml
plugins/clickhouse-offline-build/files/build.sh
plugins/clickhouse-offline-build/lib/clickhouse-offline-build-1.0.jar
```
3. 按照官方文档https://interestinglab.github.io/waterdrop-docs/#/zh-cn/v1/quick-start 准备环境
4. 使用如下output的配置
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

### 本地运行示例

0. 先确保clickhouse已安装, 并且`test.ontime`表已建好
1. 根据官方文档https://interestinglab.github.io/waterdrop-docs/#/zh-cn/v1/installation下载到waterdrop并解压
2. 准备少量`ontime`的数据: https://clickhouse.tech/docs/en/getting-started/example-datasets/ontime/
3. 创建文件config/application.conf
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

4. 本地执行命令
```shell
./bin/start-waterdrop.sh --config config/application.conf --deploy-mode client --master "local[2]"
```

5. 成功后数据在`hdfs://remote_hdfs_host:9000/clickhouse-build/test/ontime/`
6. 到clickhouse节点上执行: `sh attach.sh test ontime hdfs://remote_hdfs_host:9000/clickhouse-build/test/ontime/`
7. 验证数据`select count(*) from test.ontime`