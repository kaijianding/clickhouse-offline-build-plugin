package io.github.interestinglab.waterdrop.output.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.github.interestinglab.waterdrop.apis.BaseOutput;
import io.github.interestinglab.waterdrop.config.Common;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigRuntimeException;
import io.github.interestinglab.waterdrop.config.ConfigValue;
import io.github.interestinglab.waterdrop.config.TypesafeConfigUtils;
import io.github.interestinglab.waterdrop.utils.CompressionUtils;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import ru.yandex.clickhouse.ClickHouseDataSource;
import scala.Tuple2;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ClickhouseOfflineBuild extends BaseOutput
{
  private Config config;

  private final Properties properties = new Properties();

  private static final String clickhousePrefix = "clickhouse.";

  private Map<String, String> tableSchema;
  private String createTargetTableSql;

  private String database;
  private String table;
  private Map<String, Object> defaultValues;
  private String tmpUploadPath;
  private String hdfsUser;

  @Override
  public Config getConfig()
  {
    return config;
  }

  @Override
  public void setConfig(Config config)
  {
    this.config = config;
  }

  @Override
  public Tuple2<Object, String> checkConfig()
  {
    List<String> requiredOptions = ImmutableList.of("host", "table", "database", "tmpUploadPath");

    for (String require : requiredOptions) {
      if (!config.hasPath(require)) {
        throw new ConfigRuntimeException("option " + require + " is required");
      }
    }

    if (TypesafeConfigUtils.hasSubConfig(config, clickhousePrefix)) {
      Config clickhouseConfig = TypesafeConfigUtils.extractSubConfig(config, clickhousePrefix, false);
      for (Map.Entry<String, ConfigValue> e : clickhouseConfig.entrySet()) {
        properties.put(e.getKey(), String.valueOf(e.getValue().unwrapped()));
      }
    }

    boolean hasUserName = config.hasPath("username");
    boolean hasPassword = config.hasPath("password");

    if ((hasUserName && !hasPassword) || (!hasUserName && hasPassword)) {
      throw new ConfigRuntimeException("please specify username and password at the same time");
    }
    if (hasPassword) {
      properties.put("user", config.getString("username"));
      properties.put("password", config.getString("password"));
      hdfsUser = config.getString("username");
      if (config.hasPath("hdfsUser")) {
        hdfsUser = config.getString("hdfsUser");
      }
    }
    return new Tuple2<>(true, "");
  }

  @Override
  public void prepare(SparkSession spark)
  {
    database = config.getString("database");
    table = config.getString("table");
    tmpUploadPath = config.getString("tmpUploadPath");
    if (!tmpUploadPath.endsWith("/")) {
      tmpUploadPath = tmpUploadPath + "/";
    }
    String jdbcLink = String.format("jdbc:clickhouse://%s/%s", config.getString("host"), database);

    DataSource dataSource = new ClickHouseDataSource(jdbcLink, properties);
    try {
      Connection conn = dataSource.getConnection();
      tableSchema = getClickHouseSchema(conn, table);

      String createTableSql = getClickHouseCreateTableSql(conn, table);
      conn.close();
      createTargetTableSql = buildCreateTargetTableSql(createTableSql);
    }
    catch (SQLException throwables) {
      throw new RuntimeException("failed to get table schema", throwables);
    }
    defaultValues = config.hasPath("defaultValues") ? config.getObject("defaultValues").unwrapped() : new HashMap<>();
  }

  @Override
  public void process(Dataset<Row> df)
  {
    final String binaryDir = Paths.get(Common.pluginFilesDir("clickhouse-offline-build").toString()).toString();
    try {
      final String[] dfFields = df.schema().fieldNames();
      // 1. create input table to receive data from stdin,
      // 2. create target database
      // 3. create target table
      // 4. insert into target table from the input table
      String fullTableName = database + "." + table;
      final String buildSql = buildInputSql(fullTableName, dfFields, tableSchema)
                              + ";\n"
                              + "create database if not exists " + database + ";\n"
                              + createTargetTableSql
                              + ";\n"
                              + buildInsertSql(fullTableName, dfFields)
                              + ";\noptimize table " + fullTableName + " final";

      boolean clusterMode = "cluster".equals(Common.getDeployMode().get());
      df.foreachPartition(new ForeachPartitionFunction<Row>()
      {
        @Override
        public void call(Iterator<Row> rows) throws Exception
        {
          File workDir = Files.createTempDir();
          File current = new File(".");
          if (clusterMode) {
            unzipPlugins(current);
          }
          PartBuilder builder = new PartBuilder(
              log(),
              clusterMode ? current.getAbsolutePath() + "/" + binaryDir : binaryDir,
              workDir,
              dfFields,
              tableSchema,
              defaultValues,
              buildSql
          );
          try {
            // 5. write to pipe file and wait fot part files be built
            builder.build(rows);

            // this path is defined in config.xml
            String dataDir = "clickhouse_data/data/data/" + database + "/" + table + "/";
            PartUploader uploader = new PartUploader(
                log(),
                hdfsUser,
                tmpUploadPath,
                new File(workDir, dataDir)
            );
            try {
              // 6. zip and upload part files to hdfs as $tmpUploadPath/$uuid_$part.zip
              uploader.upload();
            }
            catch (Exception e) {
              // try clean if upload failed
              uploader.clean();
              throw e;
            }
          }
          catch (Exception e) {
            log().info(e.getMessage(), e);
            throw e;
          }
          finally {
            FileUtils.deleteDirectory(workDir);
          }
        }
      });
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void unzipPlugins(File workDir) throws IOException, ArchiveException
  {
    log().info("preparing cluster mode work dir files...");

    // decompress plugin dir
    File compressedFile = new File(workDir, "plugins.tar.gz");

    File tempFile = CompressionUtils.unGzip(compressedFile, workDir);
    CompressionUtils.unTar(tempFile, workDir);
  }

  private Map<String, String> getClickHouseSchema(Connection conn, String table) throws SQLException
  {
    String sql = "desc " + table;
    ResultSet resultSet = conn.createStatement().executeQuery(sql);
    Map<String, String> schema = new HashMap<>();
    while (resultSet.next()) {
      schema.put(resultSet.getString(1), resultSet.getString(2));
    }
    return schema;
  }

  private String getClickHouseCreateTableSql(Connection conn, String table) throws SQLException
  {
    String sql = "show create table " + table;
    ResultSet resultSet = conn.createStatement().executeQuery(sql);
    String createSql = null;
    if (resultSet.next()) {
      createSql = resultSet.getString(1);
    }
    return createSql;
  }

  private String buildInputSql(String table, String[] fields, Map<String, String> tableSchema)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("CREATE TABLE input (\n");
    for (int i = 0; i < fields.length; i++) {
      String field = fields[i];
      sb.append("`");
      sb.append(field);
      sb.append("` ");
      String type = tableSchema.get(field);
      if (type == null) {
        throw new ConfigRuntimeException(field + " is not valid column in table " + table);
      }
      sb.append(type);
      if (i < fields.length - 1) {
        sb.append(",\n");
      }
    }
    // the PartBuild will write data to pipe using CSV format
    sb.append("\n) ENGINE = File(CSV, stdin);");
    return sb.toString();
  }

  private String buildCreateTargetTableSql(String inSql)
  {
    return inSql
        // use MergeTree because we only use it as local table
        .replaceAll("ENGINE\\s*=\\s*\\w+\\s*\\(.*?\\)", "ENGINE = MergeTree()")
        // remove storage_policy because we don't have one
        .replaceAll(",\\s*storage_policy\\s*=\\s*'\\w+'", "");
  }

  private String buildInsertSql(String fullTableName, String[] fields)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("insert into ");
    sb.append(fullTableName);
    sb.append(" (");
    for (int i = 0; i < fields.length; i++) {
      String field = fields[i];
      sb.append("`");
      sb.append(field);
      sb.append("`");
      if (i < fields.length - 1) {
        sb.append(",");
      }
    }
    sb.append(") select * from input");
    return sb.toString();
  }
}
